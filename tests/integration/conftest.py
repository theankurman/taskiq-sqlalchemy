import pytest
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture(scope="session")
def db_engine():
    return create_async_engine("sqlite+aiosqlite:///:memory:")
