from enum import Enum


class TableRestoreStatusStatus(str, Enum):
    COMMITTED = "committed"
    DURABILITY_PENDING = "durability_pending"
    FAILED = "failed"
    SKIPPED = "skipped"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
