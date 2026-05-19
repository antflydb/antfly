from enum import Enum


class EmbeddingsIndexStatsIndexType(str, Enum):
    EMBEDDINGS = "embeddings"

    def __str__(self) -> str:
        return str(self.value)
