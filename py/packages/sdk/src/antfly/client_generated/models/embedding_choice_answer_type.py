from enum import StrEnum


class EmbeddingChoiceAnswerType(StrEnum):
    CHOICE = "choice"

    def __str__(self) -> str:
        return str(self.value)
