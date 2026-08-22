from enum import Enum


class ClusterBackupResponseStatus(str, Enum):
    AMBIGUOUS = "ambiguous"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"
    SUCCESSFUL = "successful"

    def __str__(self) -> str:
        return str(self.value)
