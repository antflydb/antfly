from enum import StrEnum


class IdempotentBatchErrorStatus(StrEnum):
    ABORTED = "aborted"
    NOT_APPLIED = "not_applied"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
