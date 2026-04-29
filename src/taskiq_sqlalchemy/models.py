from datetime import datetime
import enum
from typing import Any

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column


class ResultTableMixin:
    task_id: Mapped[str] = mapped_column(primary_key=True)
    result: Mapped[bytes]


class MessageTableMixin:
    class StatusChoices(enum.StrEnum):
        PENDING = "pending"
        PROCESSING = "processing"
        DONE = "done"

    created_at: Mapped[datetime] = mapped_column(server_default=sa.func.now())
    task_id: Mapped[str] = mapped_column(primary_key=True)
    task_name: Mapped[str]

    message: Mapped[bytes]
    labels: Mapped[dict[str, Any]] = mapped_column(sa.JSON)

    status: Mapped[StatusChoices] = mapped_column(default=StatusChoices.PENDING)

    delay_to: Mapped[datetime]
    priority: Mapped[int]

    claimed_by: Mapped[str | None]
