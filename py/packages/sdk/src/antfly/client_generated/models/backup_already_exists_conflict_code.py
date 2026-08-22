from enum import Enum


class BackupAlreadyExistsConflictCode(str, Enum):
    BACKUP_ALREADY_EXISTS = "backup_already_exists"

    def __str__(self) -> str:
        return str(self.value)
