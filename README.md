# TaskIQ - SQLAlchemy

TaskIQ-SQLAlchemy is a plugin for taskiq that adds a new broker and a new result backend based on SQLAlchemy.

The broker uses polling to fetch messages.


## Usage
```python
import asyncio

from taskiq_sqlalchemy import SQLAlchemyBroker, SQLAlchemyResultBackend

# can be a sqlalchemy connection string or a instance of sqlalchemy Engine or AsyncEngine
connection_string = "sqlite:///tasks.db"


result_backend = SQLAlchemyResultBackend(connection_string)

broker = SQLAlchemyBroker(connection_string).with_result_backend(result_backend)

@broker.task()
async def my_add_task(a:int|float, b:int|float) -> int|float:
    await asyncio.sleep(1)
    return a + b

async def main():
    await broker.startup()
    task = await my_add_task.kiq(1, 1)
    result = await task.wait_result()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
```

## Tests
This library has been tested against the following database and drivers:
- SQLite: sqlite(sync), aiosqlite(async)
- Postgres: psycopg(sync), psycopg(async)
- Mysql: pymysql(sync)
- SQLServer: pymssql(sync)

## Considerations
Although both sync and async engines are supported, you should prefer to use async engines where possible as the sync methods block while reading the database.

