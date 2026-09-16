from enum import StrEnum


class ExtractionConstraintIffType(StrEnum):
    IFF = "Iff"

    def __str__(self) -> str:
        return str(self.value)
