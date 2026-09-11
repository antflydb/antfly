from enum import StrEnum


class ExtractionConstraintCardinalityType(StrEnum):
    CARDINALITY = "Cardinality"

    def __str__(self) -> str:
        return str(self.value)
