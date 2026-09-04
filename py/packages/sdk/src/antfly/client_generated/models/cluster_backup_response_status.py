from enum import StrEnum


class ClusterBackupResponseStatus(StrEnum):
    AMBIGUOUS = "ambiguous"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"
    SUCCESSFUL = "successful"

    def __str__(self) -> str:
        return str(self.value)
