from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DBBase(DeclarativeBase):
    pass


class ResultTableMixin(DeclarativeBase):
    task_id: Mapped[str] = mapped_column(primary_key=True)
    result: Mapped[bytes]
