from enum import Enum


class BatchResponseStatus(str, Enum):
    COMMITTED = "committed"
    COMMITTED_PENDING = "committed_pending"
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"

    def __str__(self) -> str:
        return str(self.value)
