from enum import Enum


class RestoreJobResultStatus(str, Enum):
    COMPLETED = "completed"
    DURABILITY_PENDING = "durability_pending"
    FAILED = "failed"
    PARTIAL = "partial"

    def __str__(self) -> str:
        return str(self.value)
