from enum import StrEnum


class CreateEmbeddingsIndexRequestType(StrEnum):
    EMBEDDINGS = "embeddings"

    def __str__(self) -> str:
        return str(self.value)
