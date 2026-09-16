from enum import StrEnum


class ExtractionConstraintIsDefaultType(StrEnum):
    ISDEFAULT = "IsDefault"

    def __str__(self) -> str:
        return str(self.value)
