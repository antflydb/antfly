from enum import StrEnum


class ExtractionConstraintAndType(StrEnum):
    AND = "And"

    def __str__(self) -> str:
        return str(self.value)
