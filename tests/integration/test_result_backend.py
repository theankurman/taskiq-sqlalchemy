import secrets

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from taskiq import AsyncBroker, InMemoryBroker
from taskiq.exceptions import ResultGetError
from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend


async def _broker(db_engine, keep_results: bool):
    result_backend = SQLAlchemyResultBackend(db_engine, keep_results=keep_results)
    broker = InMemoryBroker().with_result_backend(result_backend)
    await result_backend.startup()
    return broker


@pytest.fixture()
async def broker_keep(db_engine):
    return await _broker(db_engine, True)


@pytest.fixture
async def broker_no_keep(db_engine):
    return await _broker(db_engine, False)


async def check_table_exists(db_engine: AsyncEngine | Engine, table_name: str):
    meta = sa.MetaData()
    if isinstance(db_engine, Engine):
        meta.reflect(db_engine)
    else:
        async with db_engine.connect() as conn:
            await conn.run_sync(meta.reflect)
    return table_name in meta.tables


async def test_result_table_created(db_engine: AsyncEngine):
    table_name = secrets.token_hex(16)
    backend = SQLAlchemyResultBackend(db_engine, table_name=table_name)

    # get list of tables
    assert not await check_table_exists(db_engine, table_name)

    # WHEN: startup called
    await backend.startup()

    # THEN: table created
    assert await check_table_exists(db_engine, table_name)


async def test_result_stored(broker_keep: AsyncBroker):
    val = secrets.token_hex()

    @broker_keep.task
    async def _test_task():
        return val

    # WHEN: task is processed
    kicker = await _test_task.kiq()
    print("kicker sent")
    result = await kicker.wait_result()
    print("result got")

    # THEN: result is stored
    assert not result.is_err
    assert result.return_value == val


async def test_result_removed_on_access(
    broker_no_keep: AsyncBroker,
):
    # GIVEN: keep_results = False

    @broker_no_keep.task
    def _test_task():
        return 1

    kicker = await _test_task.kiq()

    # WHEN: result is accessed
    result = await kicker.wait_result()
    assert not result.is_err

    # THEN: result should be deleted
    with pytest.raises(ResultGetError):
        _result2 = await kicker.get_result()


async def test_result_kept_on_access(
    broker_keep: AsyncBroker,
):
    # GIVEN: keep_results = True
    @broker_keep.task
    def _test_task():
        return 1

    kicker = await _test_task.kiq()
    # WHEN: result is accessed
    result = await kicker.wait_result()
    assert not result.is_err

    # THEN: result should not be deleted
    result2 = await kicker.get_result()
    assert not result2.is_err
