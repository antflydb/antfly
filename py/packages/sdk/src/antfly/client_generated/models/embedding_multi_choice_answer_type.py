from enum import StrEnum


class EmbeddingMultiChoiceAnswerType(StrEnum):
    MULTI_CHOICE = "multi_choice"

    def __str__(self) -> str:
        return str(self.value)
