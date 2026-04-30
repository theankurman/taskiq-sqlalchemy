import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(
    scope="session",
    params=[
        "sqlite",
        "sqlite_sync",
        "postgres",
        "postgres_sync",
    ],
)
def db_engine(request):
    dbname = request.param
    match dbname:
        case "sqlite":
            db = None
            url = "sqlite+aiosqlite:///:memory:"
            engine = create_async_engine(url)
        case "sqlite_sync":
            db = None
            url = "sqlite:///:memory:"
            engine = create_engine(url)
        case "postgres":
            db = PostgresContainer()
            db.start()
            url = db.get_connection_url(driver="psycopg")
            engine = create_async_engine(url)
        case "postgres_sync":
            db = PostgresContainer()
            db.start()
            url = db.get_connection_url(driver="psycopg")
            engine = create_engine(url)
        case _:
            raise Exception("Unsupported database type")

    def _cleanup():
        stop_function = getattr(db, "stop", None)
        if stop_function:
            stop_function()

    request.addfinalizer(_cleanup)
    return engine
