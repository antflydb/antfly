from enum import StrEnum


class InferenceEmbeddingObjectObject(StrEnum):
    EMBEDDING = "embedding"

    def __str__(self) -> str:
        return str(self.value)
