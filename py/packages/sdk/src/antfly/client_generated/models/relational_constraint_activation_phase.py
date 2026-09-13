from enum import StrEnum


class RelationalConstraintActivationPhase(StrEnum):
    FOREIGN_KEY = "foreign_key"
    UNIQUE = "unique"

    def __str__(self) -> str:
        return str(self.value)
