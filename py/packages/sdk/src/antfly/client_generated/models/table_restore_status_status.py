from enum import Enum


class TableRestoreStatusStatus(str, Enum):
    FAILED = "failed"
    SKIPPED = "skipped"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
