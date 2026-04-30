import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import sqlalchemy
import sqlalchemy as sa
import sqlalchemy.exc
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase, declarative_base
from taskiq import BrokerMessage
from taskiq.abc import AsyncBroker
from taskiq.abc.serializer import TaskiqSerializer
from taskiq.acks import AckableMessage
from taskiq.serializers import PickleSerializer

from taskiq_sqlalchemy.models import MessageTableMixin


class SQLAlchemyBroker(AsyncBroker):
    def __init__(
        self,
        connection_string: str | AsyncEngine | Engine,
        table_name: str = "taskiq_messages",
        serializer: TaskiqSerializer | None = None,
    ) -> None:
        """
        Construct a new SQLAlchemyBroker.

        Args:
            connection_string: sqlalchemy connection string, or an instance of sqlalchemy Engine or AsyncEngine.
            table_name: the name of the table used to store messages.
            serializer: the serializer class used to (de)serialize the message instance.
        """
        super().__init__()

        if isinstance(connection_string, str):
            try:
                connection_string = create_async_engine(connection_string)
            except sqlalchemy.exc.InvalidRequestError:
                connection_string = create_engine(connection_string)
        self.engine = connection_string
        self.serializer = serializer or PickleSerializer()

        self._create_task_model(table_name)

    def _create_task_model(self, table_name: str):
        self.db_base: type[DeclarativeBase] = declarative_base(class_registry={})

        class MessageModel(self.db_base, MessageTableMixin):
            __tablename__ = table_name

        self.model = MessageModel

    async def startup(self) -> None:
        """
        Initialize the broker.

        Creates the message table if it does not exist.
        """
        if isinstance(self.engine, Engine):
            with self.engine.begin() as conn:
                self.db_base.metadata.create_all(conn)
        else:
            async with self.engine.begin() as conn:
                await conn.run_sync(self.db_base.metadata.create_all)

        await super().startup()

    async def kick(self, message: BrokerMessage) -> None:
        _delay_seconds = float(message.labels.get("delay", 0))
        print(message.task_id)
        stmt = sa.insert(self.model).values(
            {
                self.model.task_id: message.task_id,
                self.model.task_name: message.task_name,
                self.model.message: message.message,
                self.model.labels: message.labels,
                self.model.priority: int(message.labels.get("priority", 0)),
                self.model.delay_to: datetime.now(UTC)
                + timedelta(seconds=_delay_seconds),
            }
        )
        if isinstance(self.engine, Engine):
            with self.engine.begin() as conn:
                conn.execute(stmt)
        else:
            async with self.engine.begin() as conn:
                await conn.execute(stmt)

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:

        select_statement = (
            sa.select(self.model.task_id)
            .where(
                self.model.delay_to <= sa.bindparam("now"),
                self.model.status == self.model.StatusChoices.PENDING,
                self.model.claimed_by.is_(None),
            )
            .order_by(
                self.model.priority.desc(),
                self.model.created_at,
            )
            .limit(1)
            .with_for_update(skip_locked=True)
        )

        def _get_claim_statement(task_id, worker_id):
            return (
                sa.update(self.model)
                .where(self.model.task_id == task_id)
                .values({self.model.claimed_by: worker_id})
            )

        def _get_bytes_statement(task_id, worker_id):
            return sa.select(self.model.message).where(
                self.model.task_id == task_id,
                self.model.claimed_by == worker_id,
                self.model.status == self.model.StatusChoices.PENDING,
            )

        def _get_update_status_statement(task_id, worker_id):
            return (
                sa.update(self.model)
                .where(
                    self.model.task_id == task_id, self.model.claimed_by == worker_id
                )
                .values({self.model.status: self.model.StatusChoices.PROCESSING})
            )

        def _get_ack_statement(task_id):
            return (
                sa.update(self.model)
                .where(self.model.task_id == task_id)
                .values({self.model.status: self.model.StatusChoices.DONE})
            )

        worker_id = uuid7().hex
        while True:
            if isinstance(self.engine, Engine):
                with self.engine.begin() as conn:
                    # get
                    task_id = conn.execute(
                        select_statement.params(now=datetime.now(UTC))
                    ).scalar_one_or_none()
                    if not task_id:
                        await asyncio.sleep(1)
                        continue
                    # claim
                    conn.execute(_get_claim_statement(task_id, worker_id))
                    # get bytes
                    message_bytes = conn.execute(
                        _get_bytes_statement(task_id, worker_id)
                    ).scalar_one_or_none()
                    if not message_bytes:
                        await asyncio.sleep(1)
                        continue
                    # update status
                    conn.execute(_get_update_status_statement(task_id, worker_id))
            else:
                async with self.engine.begin() as conn:
                    # get
                    task_id = (
                        await conn.execute(
                            select_statement.params(now=datetime.now(UTC))
                        )
                    ).scalar_one_or_none()
                    if not task_id:
                        await asyncio.sleep(1)
                        continue
                    # claim
                    await conn.execute(_get_claim_statement(task_id, worker_id))
                    # get bytes
                    message_bytes = (
                        await conn.execute(_get_bytes_statement(task_id, worker_id))
                    ).scalar_one_or_none()
                    if not message_bytes:
                        await asyncio.sleep(1)
                        continue
                    # update status
                    await conn.execute(_get_update_status_statement(task_id, worker_id))

            async def _ack(task_id=task_id):
                if isinstance(self.engine, Engine):
                    with self.engine.begin() as conn:
                        conn.execute(_get_ack_statement(task_id))
                else:
                    async with self.engine.begin() as conn:
                        await conn.execute(_get_ack_statement(task_id))

            yield AckableMessage(data=message_bytes, ack=_ack)
