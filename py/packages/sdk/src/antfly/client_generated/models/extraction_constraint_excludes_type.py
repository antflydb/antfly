from enum import StrEnum


class ExtractionConstraintExcludesType(StrEnum):
    EXCLUDES = "Excludes"

    def __str__(self) -> str:
        return str(self.value)
