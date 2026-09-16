from enum import StrEnum


class ExtractionConstraintImpliesType(StrEnum):
    IMPLIES = "Implies"

    def __str__(self) -> str:
        return str(self.value)
