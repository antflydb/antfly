from enum import Enum


class ClusterRestoreResponseStatus(str, Enum):
    COMPLETED = "completed"
    DURABILITY_PENDING = "durability_pending"
    FAILED = "failed"
    PARTIAL = "partial"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
