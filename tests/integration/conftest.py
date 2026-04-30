import pytest
import sqlalchemy as sa
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.mssql import SqlServerContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer


@pytest.fixture(
    scope="session",
    params=[
        "sqlite_async",
        "sqlite_sync",
        "postgres_async",
        "postgres_sync",
        "mysql_sync",
        "mssql_sync",
    ],
)
def db_engine(request):
    dbname = request.param
    match dbname:
        case "sqlite_async":
            db = None
            url = "sqlite+aiosqlite:///:memory:"
            engine = create_async_engine(url)
        case "sqlite_sync":
            db = None
            url = "sqlite:///:memory:"
            engine = create_engine(url)
        case "postgres_async":
            db = PostgresContainer()
            db.start()
            url = db.get_connection_url(driver="psycopg")
            engine = create_async_engine(url)
        case "postgres_sync":
            db = PostgresContainer()
            db.start()
            url = db.get_connection_url(driver="psycopg")
            engine = create_engine(url)
        case "mysql_sync":
            db = MySqlContainer(dialect="pymysql")
            db.start()
            url = db.get_connection_url()
            engine = create_engine(url)
        case "mssql_sync":
            db = SqlServerContainer(
                image="mcr.microsoft.com/mssql/server:2022-latest",
                dialect="mssql+pymssql",
            )
            db.start()
            url = db.get_connection_url()
            engine = create_engine(url)
        case _:
            raise Exception("Unsupported database type")

    def _cleanup():
        stop_function = getattr(db, "stop", None)
        if stop_function:
            stop_function()

    request.addfinalizer(_cleanup)
    return engine


async def check_table_exists(db_engine: Engine | AsyncEngine, table_name: str):
    def _check_table(conn: sa.Connection, table_name: str):
        inspector = sa.inspect(conn)
        return inspector.has_table(table_name)

    if isinstance(db_engine, Engine):
        with db_engine.begin() as conn:
            return _check_table(conn, table_name)
    else:
        async with db_engine.begin() as conn:
            return await conn.run_sync(lambda c: _check_table(c, table_name))
