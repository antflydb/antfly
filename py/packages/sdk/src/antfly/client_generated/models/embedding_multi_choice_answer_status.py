from enum import StrEnum


class EmbeddingMultiChoiceAnswerStatus(StrEnum):
    ABSTAINED = "abstained"
    EMPTY = "empty"
    SELECTED = "selected"

    def __str__(self) -> str:
        return str(self.value)
