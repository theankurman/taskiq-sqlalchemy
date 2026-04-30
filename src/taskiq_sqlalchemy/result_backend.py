from typing import TypeVar, override

import sqlalchemy
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase, declarative_base
from taskiq import TaskiqResult
from taskiq.abc import AsyncResultBackend
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.compat import model_dump, model_validate
from taskiq.serializers import PickleSerializer

from .models import ResultTableMixin

_ResultType = TypeVar("_ResultType")


class SQLAlchemyResultBackend(AsyncResultBackend[_ResultType]):
    def __init__(
        self,
        connection_string: str | AsyncEngine | Engine,
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        serializer: TaskiqSerializer | None = None,
    ) -> None:
        """
        Construct a new result backend.

        Args:
            engine: sqlalchemy connection string or an instance of sqlalchemy Engine or AsyncEngine .
            keep_results: flag to not remove results after reading.
            table_name: the name of the table to create for result storage.
            serializer: the serializer class to (de)serialize the result instance.

        """
        super().__init__()

        if isinstance(connection_string, str):
            try:
                connection_string = create_async_engine(connection_string)
            except sqlalchemy.exc.InvalidRequestError:
                connection_string = create_engine(connection_string)
        self.engine = connection_string

        self.serializer = serializer or PickleSerializer()
        self.keep_results = keep_results

        self._create_result_model(table_name)

    def _create_result_model(self, table_name: str):
        self.db_base: type[DeclarativeBase] = declarative_base(class_registry={})

        class ResultModel(self.db_base, ResultTableMixin):
            __tablename__ = table_name

        self.model = ResultModel

    @override
    async def startup(self) -> None:
        """
        Initialize the result backend.

        Creates the result table if it does not exist.
        """

        if isinstance(self.engine, Engine):
            with self.engine.begin() as conn:
                self.db_base.metadata.create_all(conn)
        else:
            async with self.engine.begin() as conn:
                await conn.run_sync(lambda c: self.db_base.metadata.create_all(c))

        await super().startup()

    @override
    async def set_result(self, task_id: str, result: TaskiqResult[_ResultType]) -> None:
        """
        Set the result to the database table.

        Args:
            task_id: id of the task
            result: result of the task
        """
        result_bytes = self.serializer.dumpb(model_dump(result))

        insert_statement = sa.insert(self.model).values(
            {
                self.model.task_id: task_id,
                self.model.result: result_bytes,
            }
        )

        if isinstance(self.engine, Engine):
            with self.engine.begin() as conn:
                conn.execute(insert_statement)
        else:
            async with self.engine.begin() as conn:
                await conn.execute(insert_statement)

    @override
    async def get_result(
        self, task_id: str, with_logs: bool = False
    ) -> TaskiqResult[_ResultType]:
        """
        Gets the result from the database table.
        Also deletes the result if the broker was initialized with keep_results=False.

        Args:
            task_id: id of the task
            with_logs: does nothing (deprecated by taskiq)

        Raises:
            ResultIsMissingError: if result for the task is not found

        Returns:
            TaskiqResult
        """
        select_statement = sa.select(self.model.result).where(
            self.model.task_id == task_id
        )
        delete_statement = sa.delete(self.model).where(self.model.task_id == task_id)

        if isinstance(self.engine, Engine):
            with self.engine.begin() as conn:
                result_bytes = conn.execute(select_statement).scalar_one_or_none()

                if not self.keep_results:
                    conn.execute(delete_statement)
        else:
            async with self.engine.begin() as conn:
                result_bytes = (
                    await conn.execute(select_statement)
                ).scalar_one_or_none()

                if not self.keep_results:
                    await conn.execute(delete_statement)

        if result_bytes is None:
            raise ValueError(f"Result not found for task with id {task_id}")

        result_deserialized = self.serializer.loadb(result_bytes)
        result = model_validate(TaskiqResult[_ResultType], result_deserialized)
        return result

    @override
    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks if the result is ready.

        Args:
            task_id: id of the task

        Returns:
            `True` if the result is ready `False` otherwise
        """
        count_statement = (
            sa.select(sa.func.count())
            .select_from(self.model)
            .where(self.model.task_id == task_id)
        )
        if isinstance(self.engine, Engine):
            with self.engine.begin() as conn:
                num_rows = conn.execute(count_statement).scalar_one()
        else:
            async with self.engine.begin() as conn:
                num_rows = (await conn.execute(count_statement)).scalar_one()

        return num_rows > 0
