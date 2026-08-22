from enum import Enum


class BackupOutcomeAmbiguousConflictCode(str, Enum):
    BACKUP_OUTCOME_AMBIGUOUS = "backup_outcome_ambiguous"

    def __str__(self) -> str:
        return str(self.value)
