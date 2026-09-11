from enum import StrEnum


class BackupOutcomeAmbiguousConflictCode(StrEnum):
    BACKUP_OUTCOME_AMBIGUOUS = "backup_outcome_ambiguous"

    def __str__(self) -> str:
        return str(self.value)
