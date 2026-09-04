from enum import StrEnum


class EmbeddingsIndexStatsIndexType(StrEnum):
    EMBEDDINGS = "embeddings"

    def __str__(self) -> str:
        return str(self.value)
