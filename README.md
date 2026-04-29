# TaskIQ - SQLAlchemy

TaskIQ-SQLAlchemy is a plugin for taskiq that adds a new broker and a new result backend based on SQLAlchemy.

The broker uses polling to fetch messages.


## Usage
```python
import asyncio

from taskiq_sqlalchemy import SQLAlchemyBroker, SQLAlchemyResultBackend

# can be a sqlalchemy async compatible connection string or a sqlalchemy AsyncEngine instance
connection_string = "sqlite+aiosqlite:///tasks.db"


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