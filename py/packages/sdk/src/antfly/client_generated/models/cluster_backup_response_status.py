from enum import Enum


class ClusterBackupResponseStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"

    def __str__(self) -> str:
        return str(self.value)
