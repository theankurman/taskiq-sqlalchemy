import secrets

import pytest
import sqlalchemy as sa
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


async def test_result_table_created(db_engine: AsyncEngine):
    table_name = secrets.token_hex(16)
    backend = SQLAlchemyResultBackend(db_engine, table_name=table_name)

    # get list of tables
    async with db_engine.connect() as conn:
        meta = sa.MetaData()

        # check table does not already exist
        await conn.run_sync(meta.reflect)
        assert meta.tables.get(table_name) is None

        # WHEN: startup called
        await backend.startup()

        # THEN: table created
        await conn.run_sync(meta.reflect)
        assert meta.tables.get(table_name) is not None


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
