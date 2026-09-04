from enum import StrEnum


class TableBackupStatusStatus(StrEnum):
    AMBIGUOUS = "ambiguous"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    SUCCESSFUL = "successful"

    def __str__(self) -> str:
        return str(self.value)
