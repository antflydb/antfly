from enum import StrEnum


class ExtractionConstraintMinLevelType(StrEnum):
    MINLEVEL = "MinLevel"

    def __str__(self) -> str:
        return str(self.value)
