from enum import StrEnum


class ExtractionConstraintExactlyOneOfType(StrEnum):
    EXACTLYONEOF = "ExactlyOneOf"

    def __str__(self) -> str:
        return str(self.value)
