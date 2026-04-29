import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, declarative_base
from taskiq import BrokerMessage
from taskiq.abc import AsyncBroker
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine, async_sessionmaker

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
        self.session = async_sessionmaker(self.engine)
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

    async def kick(self, message: BrokerMessage) -> None:
        async with self.session() as session, session.begin():
            db_message = await session.scalar(
                sa.select(self.model).filter(self.model.task_id == message.task_id)
            )
            if not db_message:
                db_message = self.model()
                db_message.task_id = message.task_id
                session.add(db_message)

            db_message.task_name = message.task_name
            db_message.message = message.message
            db_message.labels = message.labels
            db_message.priority = int(message.labels.get("priority", 0))

            _delay_seconds = float(message.labels.get("delay", 0))
            db_message.delay_to = datetime.now(UTC) + timedelta(seconds=_delay_seconds)

            await session.commit()

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        while True:
            query = (
                sa.select(self.model)
                .filter(
                    self.model.delay_to <= datetime.now(UTC),
                    self.model.status == self.model.StatusChoices.PENDING,
                )
                .order_by(
                    self.model.priority.desc(),
                    self.model.created_at,
                )
                .with_for_update(skip_locked=True)
            )
            async with self.session() as session, session.begin():
                db_message = await session.scalar(query)
                if not db_message:
                    await asyncio.sleep(1)
                    continue

                db_message.status = self.model.StatusChoices.PROCESSING

                data = db_message.message
                await session.commit()

            async def _ack(m=db_message):
                async with self.session() as session, session.begin():
                    session.add(m)
                    m.status = self.model.StatusChoices.DONE
                    await session.commit()

            yield AckableMessage(data=data, ack=_ack)
