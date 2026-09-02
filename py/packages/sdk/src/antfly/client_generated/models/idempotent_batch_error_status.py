from enum import Enum


class IdempotentBatchErrorStatus(str, Enum):
    ABORTED = "aborted"
    NOT_APPLIED = "not_applied"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
