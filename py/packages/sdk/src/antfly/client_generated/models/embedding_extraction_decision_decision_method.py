from enum import StrEnum


class EmbeddingExtractionDecisionDecisionMethod(StrEnum):
    EMBEDDING_SIMILARITY = "embedding_similarity"

    def __str__(self) -> str:
        return str(self.value)
