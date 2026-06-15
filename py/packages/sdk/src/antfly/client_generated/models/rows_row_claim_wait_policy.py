from enum import Enum


class RowsRowClaimWaitPolicy(str, Enum):
    NOWAIT = "nowait"
    SKIP_LOCKED = "skip_locked"
    WAIT = "wait"

    def __str__(self) -> str:
        return str(self.value)
