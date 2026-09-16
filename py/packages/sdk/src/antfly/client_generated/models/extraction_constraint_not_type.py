from enum import StrEnum


class ExtractionConstraintNotType(StrEnum):
    NOT = "Not"

    def __str__(self) -> str:
        return str(self.value)
