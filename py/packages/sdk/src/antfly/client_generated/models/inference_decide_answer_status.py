from enum import StrEnum


class InferenceDecideAnswerStatus(StrEnum):
    ABSTAINED = "abstained"
    SELECTED = "selected"

    def __str__(self) -> str:
        return str(self.value)
