from enum import StrEnum


class EmbeddingChoiceAnswerSimilarityMetric(StrEnum):
    COSINE = "cosine"

    def __str__(self) -> str:
        return str(self.value)
