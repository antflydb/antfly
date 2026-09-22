from enum import StrEnum


class RelationalConstraintStatusCoverageKind(StrEnum):
    UNIQUE_FOREIGN_KEY_AND_CHECK = "unique_foreign_key_and_check"

    def __str__(self) -> str:
        return str(self.value)
