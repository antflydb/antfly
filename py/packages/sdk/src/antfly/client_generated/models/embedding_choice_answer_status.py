from enum import StrEnum


class EmbeddingChoiceAnswerStatus(StrEnum):
    ABSTAINED = "abstained"
    SELECTED = "selected"

    def __str__(self) -> str:
        return str(self.value)
