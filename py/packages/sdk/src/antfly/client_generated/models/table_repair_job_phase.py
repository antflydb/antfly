from enum import StrEnum


class TableRepairJobPhase(StrEnum):
    CANCELLED = "cancelled"
    COMPLETED_WITH_FAILURES = "completed_with_failures"
    FAILED = "failed"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"

    def __str__(self) -> str:
        return str(self.value)
