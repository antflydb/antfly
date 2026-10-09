from enum import StrEnum


class EmbeddingMultiChoiceAnswerDecisionMethod(StrEnum):
    EMBEDDING_SIMILARITY = "embedding_similarity"

    def __str__(self) -> str:
        return str(self.value)
