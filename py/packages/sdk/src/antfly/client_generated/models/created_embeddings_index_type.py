from enum import StrEnum


class CreatedEmbeddingsIndexType(StrEnum):
    EMBEDDINGS = "embeddings"

    def __str__(self) -> str:
        return str(self.value)
