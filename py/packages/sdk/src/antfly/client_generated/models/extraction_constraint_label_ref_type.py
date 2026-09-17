from enum import StrEnum


class ExtractionConstraintLabelRefType(StrEnum):
    LABELREF = "LabelRef"

    def __str__(self) -> str:
        return str(self.value)
