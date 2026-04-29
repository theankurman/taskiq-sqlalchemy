import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from taskiq import InMemoryBroker

from taskiq_sqlalchemy.result_backend import SQLAlchemyResultBackend


@pytest.fixture(scope="session")
def db_engine():
    return create_async_engine("sqlite+aiosqlite:///:memory:")


@pytest.fixture()
async def broker(db_engine):
    result_backend = SQLAlchemyResultBackend(db_engine)
    broker = InMemoryBroker().with_result_backend(result_backend)
    await result_backend.startup()
    return broker
