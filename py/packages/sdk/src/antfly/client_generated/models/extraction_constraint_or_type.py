from enum import StrEnum


class ExtractionConstraintOrType(StrEnum):
    OR = "Or"

    def __str__(self) -> str:
        return str(self.value)
