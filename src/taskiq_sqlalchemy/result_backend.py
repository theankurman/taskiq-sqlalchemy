from typing import TypeVar, override

from sqlalchemy.orm import DeclarativeBase, declarative_base
from taskiq import TaskiqResult
from taskiq.compat import model_dump, model_validate
from taskiq.serializers import PickleSerializer
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.abc import AsyncResultBackend
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, async_sessionmaker
import sqlalchemy as sa
from .models import ResultTableMixin
from .exceptions import ResultIsMissingError


_ResultType = TypeVar("_ResultType")


class SQLAlchemyResultBackend(AsyncResultBackend[_ResultType]):
    def __init__(
        self,
        connection_string: str | AsyncEngine,
        keep_results: bool = True,
        table_name: str = "taskiq_results",
        serializer: TaskiqSerializer | None = None,
    ) -> None:
        """
        Construct a new result backend.

        Args:
            engine: sqlalchemy asyncio-compatible connection string or an instance of sqlalchemy AsyncEngine.
            keep_results: flag to not remove results after reading.
            table_name: the name of the table to create for result storage.
            serializer: the serializer class to (de)serialize the result instance.

        """
        super().__init__()

        if isinstance(connection_string, str):
            connection_string = create_async_engine(connection_string)
        self.engine = connection_string
        self.session = async_sessionmaker(connection_string)
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
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda c: self.db_base.metadata.create_all(c))

    @override
    async def set_result(self, task_id: str, result: TaskiqResult[_ResultType]) -> None:
        """
        Set the result to the database table.

        Args:
            task_id: id of the task
            result: result of the task
        """
        result_bytes = self.serializer.dumpb(model_dump(result))
        async with self.session() as session, session.begin():
            # create or update result
            db_result = await session.scalar(
                sa.select(self.model).filter(self.model.task_id == task_id)
            )
            if db_result is None:
                db_result = self.model()
                db_result.task_id = task_id
                session.add(db_result)

            db_result.result = result_bytes
            await session.commit()

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
        async with self.session() as session, session.begin():
            db_result = await session.scalar(
                sa.select(self.model).filter(self.model.task_id == task_id)
            )
            if not db_result:
                raise ResultIsMissingError(
                    f"Result not found for task with id {task_id}"
                )
            result_bytes = db_result.result
            result_deserialized = self.serializer.loadb(result_bytes)
            result = model_validate(TaskiqResult[_ResultType], result_deserialized)

            if not self.keep_results:
                await session.delete(db_result)
                await session.commit()

            return result

        return await super().get_result(task_id, with_logs)

    @override
    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks if the result is ready.

        Args:
            task_id: id of the task

        Returns:
            `True` if the result is ready `False` otherwise
        """
        async with self.session() as session, session.begin():
            task = await session.scalar(
                sa.select(self.model.task_id).filter(self.model.task_id == task_id)
            )
            return task is not None
