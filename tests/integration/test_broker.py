import asyncio
import secrets

import pytest
from sqlalchemy import Engine
from taskiq import AckableMessage, AsyncBroker
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine
from taskiq_sqlalchemy.broker import SQLAlchemyBroker


@pytest.fixture
async def broker(db_engine):
    b = SQLAlchemyBroker(db_engine)
    await b.startup()
    return b


@pytest.fixture
async def task(broker: AsyncBroker):
    @broker.task
    async def _task():
        return 1

    return _task


async def get_next_message(broker, timeout=1):
    async def _get() -> AckableMessage:
        message = await anext(broker.listen())
        return message

    return await asyncio.wait_for(_get(), timeout)


async def check_table_exists(db_engine: Engine | AsyncEngine, table_name: str):
    meta = sa.MetaData()
    if isinstance(db_engine, Engine):
        meta.reflect(db_engine)
    else:
        async with db_engine.begin() as conn:
            await conn.run_sync(meta.reflect)

    return table_name in meta.tables


async def test_message_table_created(db_engine: Engine | AsyncEngine):
    table_name = secrets.token_hex(16)

    # check table does not exist
    assert not await check_table_exists(db_engine, table_name)

    # WHEN: broker initialized
    broker = SQLAlchemyBroker(db_engine, table_name=table_name)
    await broker.startup()

    # THEN: table created
    assert await check_table_exists(db_engine, table_name)


async def test_message_saved(broker, task):
    # WHEN: task awaited
    res = await task.kiq()

    # THEN: message saved
    message = await get_next_message(broker)
    assert isinstance(message, AckableMessage)


async def test_message_removed(broker, task):
    # GIVEN: task awaited
    await task.kiq()

    # WHEN: message retrieved
    message = await get_next_message(broker)
    assert isinstance(message, AckableMessage)

    # THEN: message_removed
    with pytest.raises(TimeoutError):
        await get_next_message(broker)


async def test_delayed_message(broker, task):
    # WHEN: delayed task awaited
    await task.kicker().with_labels(delay=2).kiq()

    # THEN: message should not exist immediately
    with pytest.raises(TimeoutError):
        await get_next_message(broker)

    # THEN: message should exist after delay
    message = await get_next_message(broker, timeout=2)
    assert isinstance(message, AckableMessage)


async def test_message_priority(broker, task):
    # GIVEN: task with default/lower priority awaited
    await task.kicker().with_task_id("first").kiq()

    # WHEN: task with higher priority awaited
    await task.kicker().with_labels(priority=10).with_task_id("second").kiq()

    # THEN: first task is the one with higher priority
    message = await get_next_message(broker)
    data: dict = broker.serializer.loadb(message.data)
    assert data["task_id"] == "second"

    # THEN: second task is the one with lower priority
    message = await get_next_message(broker)
    data: dict = broker.serializer.loadb(message.data)
    assert data["task_id"] == "first"
