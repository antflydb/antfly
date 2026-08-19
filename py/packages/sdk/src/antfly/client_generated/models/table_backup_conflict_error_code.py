from enum import Enum


class TableBackupConflictErrorCode(str, Enum):
    BACKUP_ALREADY_EXISTS = "backup_already_exists"
    BACKUP_OUTCOME_AMBIGUOUS = "backup_outcome_ambiguous"
    TABLE_CATALOG_CHANGED = "table_catalog_changed"

    def __str__(self) -> str:
        return str(self.value)
