import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(
    scope="session",
    params=[
        "sqlite",
        "postgres",
    ],
)
def db_engine(request):
    dbname = request.param
    match dbname:
        case "sqlite":
            db = None
            url = "sqlite+aiosqlite:///:memory:"
        case "postgres":
            db = PostgresContainer()
            db.start()
            url = db.get_connection_url(driver="psycopg")
        case _:
            raise Exception("Unsupported database type")

    def _cleanup():
        stop_function = getattr(db, "stop", None)
        if stop_function:
            stop_function()

    request.addfinalizer(_cleanup)
    return create_async_engine(url)
