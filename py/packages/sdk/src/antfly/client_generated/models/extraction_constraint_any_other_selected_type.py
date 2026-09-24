from enum import StrEnum


class ExtractionConstraintAnyOtherSelectedType(StrEnum):
    ANYOTHERSELECTED = "AnyOtherSelected"

    def __str__(self) -> str:
        return str(self.value)
