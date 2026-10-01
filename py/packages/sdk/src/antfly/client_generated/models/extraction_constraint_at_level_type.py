from enum import StrEnum


class ExtractionConstraintAtLevelType(StrEnum):
    ATLEVEL = "AtLevel"

    def __str__(self) -> str:
        return str(self.value)
