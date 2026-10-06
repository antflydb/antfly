from enum import StrEnum


class RelationalConstraintRetirementStatusPhase(StrEnum):
    FENCING = "fencing"
    FOREIGN_KEYS = "foreign_keys"
    PUBLISHED = "published"
    PUBLISHING = "publishing"
    READY_TO_DROP = "ready_to_drop"
    UNIQUE = "unique"

    def __str__(self) -> str:
        return str(self.value)
