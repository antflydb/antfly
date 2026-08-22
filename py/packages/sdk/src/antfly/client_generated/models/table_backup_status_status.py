from enum import Enum


class TableBackupStatusStatus(str, Enum):
    AMBIGUOUS = "ambiguous"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    SUCCESSFUL = "successful"

    def __str__(self) -> str:
        return str(self.value)
