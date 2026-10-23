from enum import StrEnum


class DecisionEvidenceTextType(StrEnum):
    INPUT_TEXT = "input_text"

    def __str__(self) -> str:
        return str(self.value)
