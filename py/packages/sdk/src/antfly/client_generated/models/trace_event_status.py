from enum import StrEnum


class TraceEventStatus(StrEnum):
    CANCELLED = "cancelled"
    FAILED = "failed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"

    def __str__(self) -> str:
        return str(self.value)
