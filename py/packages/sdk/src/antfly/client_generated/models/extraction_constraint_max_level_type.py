from enum import StrEnum


class ExtractionConstraintMaxLevelType(StrEnum):
    MAXLEVEL = "MaxLevel"

    def __str__(self) -> str:
        return str(self.value)
