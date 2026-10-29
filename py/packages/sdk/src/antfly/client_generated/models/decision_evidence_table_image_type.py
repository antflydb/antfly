from enum import StrEnum


class DecisionEvidenceTableImageType(StrEnum):
    INPUT_IMAGE = "input_image"

    def __str__(self) -> str:
        return str(self.value)
