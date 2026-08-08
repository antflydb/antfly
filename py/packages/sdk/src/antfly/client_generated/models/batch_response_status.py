from enum import Enum


class BatchResponseStatus(str, Enum):
    COMMITTED = "committed"
    COMMITTED_PENDING = "committed_pending"

    def __str__(self) -> str:
        return str(self.value)
