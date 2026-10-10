from enum import StrEnum


class EmbeddingMultiChoiceAnswerSimilarityMetric(StrEnum):
    COSINE = "cosine"

    def __str__(self) -> str:
        return str(self.value)
