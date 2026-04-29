import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, declarative_base
from taskiq import BrokerMessage
from taskiq.abc import AsyncBroker
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from taskiq.abc.serializer import TaskiqSerializer
from taskiq.acks import AckableMessage
from taskiq.serializers import PickleSerializer

from taskiq_sqlalchemy.models import MessageTableMixin


class SQLAlchemyBroker(AsyncBroker):
    def __init__(
        self,
        connection_string: str | AsyncEngine,
        table_name: str = "taskiq_messages",
        serializer: TaskiqSerializer | None = None,
    ) -> None:
        """
        Construct a new SQLAlchemyBroker.

        Args:
            connection_string: sqlalchemy asyncio compatible connection string, or an instance of sqlalchemy AsyncEngine.
            table_name: the name of the table used to store messages.
            serializer: the serializer class used to (de)serialize the message instance.
        """
        super().__init__()

        self.engine = (
            create_async_engine(connection_string)
            if isinstance(connection_string, str)
            else connection_string
        )
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
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda c: self.db_base.metadata.create_all(c))

        await super().startup()

    async def kick(self, message: BrokerMessage) -> None:
        _delay_seconds = float(message.labels.get("delay", 0))
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
        async with self.engine.begin() as conn:
            await conn.execute(stmt)

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        while True:
            async with self.engine.begin() as conn:
                cursor_result = await conn.execute(
                    sa.select(self.model.task_id)
                    .where(
                        self.model.delay_to <= datetime.now(UTC),
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
                row = cursor_result.first()
                if row is None:
                    await asyncio.sleep(1)
                    continue

                (task_id,) = row._tuple()

                worker_id = uuid7().hex
                # the with for update does not work on sqlite
                # claim the task, so multiple workers dont work on the same task
                await conn.execute(
                    sa.update(self.model)
                    .where(self.model.task_id == task_id)
                    .values({self.model.claimed_by: worker_id})
                )

                # see if the task is still available
                cursor_result = await conn.execute(
                    sa.select(self.model.message).where(
                        self.model.task_id == task_id,
                        self.model.claimed_by == worker_id,
                        self.model.status == self.model.StatusChoices.PENDING,
                    )
                )
                row = cursor_result.first()
                if not row:
                    # another worker got the task
                    continue

                (message_bytes,) = row._tuple()

                # update message status
                await conn.execute(
                    sa.update(self.model)
                    .where(
                        self.model.task_id == task_id,
                        self.model.claimed_by == worker_id,
                    )
                    .values({self.model.status: self.model.StatusChoices.PROCESSING})
                )

            async def _ack(task_id=task_id):
                async with self.engine.begin() as conn:
                    await conn.execute(
                        sa.update(self.model)
                        .where(self.model.task_id == task_id)
                        .values({self.model.status: self.model.StatusChoices.DONE})
                    )

            yield AckableMessage(data=message_bytes, ack=_ack)
