from enum import StrEnum


class ExtractionConstraintAnySelectedType(StrEnum):
    ANYSELECTED = "AnySelected"

    def __str__(self) -> str:
        return str(self.value)
