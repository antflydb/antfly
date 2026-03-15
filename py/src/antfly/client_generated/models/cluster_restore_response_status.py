from enum import Enum


class ClusterRestoreResponseStatus(str, Enum):
    FAILED = "failed"
    PARTIAL = "partial"
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
