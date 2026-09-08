from enum import StrEnum


class RestoreJobResultStatus(StrEnum):
    COMPLETED = "completed"
    DURABILITY_PENDING = "durability_pending"
    FAILED = "failed"
    PARTIAL = "partial"

    def __str__(self) -> str:
        return str(self.value)
