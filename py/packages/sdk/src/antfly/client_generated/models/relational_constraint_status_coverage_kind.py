from enum import StrEnum


class RelationalConstraintStatusCoverageKind(StrEnum):
    UNIQUE_AND_FOREIGN_KEY = "unique_and_foreign_key"

    def __str__(self) -> str:
        return str(self.value)
