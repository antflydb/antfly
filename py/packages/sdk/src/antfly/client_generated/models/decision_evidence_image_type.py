from enum import StrEnum


class DecisionEvidenceImageType(StrEnum):
    INPUT_IMAGE = "input_image"

    def __str__(self) -> str:
        return str(self.value)
