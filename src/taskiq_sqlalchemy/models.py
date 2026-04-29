from sqlalchemy.orm import Mapped, mapped_column


class ResultTableMixin:
    task_id: Mapped[str] = mapped_column(primary_key=True)
    result: Mapped[bytes]
