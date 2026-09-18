from enum import StrEnum


class RelationalConstraintConflictReason(StrEnum):
    FOREIGN_KEY_PARENT_MISSING = "foreign_key_parent_missing"
    FOREIGN_KEY_REFERENCED = "foreign_key_referenced"
    UNIQUE_CONSTRAINT_VIOLATION = "unique_constraint_violation"

    def __str__(self) -> str:
        return str(self.value)
