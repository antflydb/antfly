from enum import Enum


class EmbeddingIndexActivityPhase(str, Enum):
    EMBEDDING = "embedding"
    IDLE = "idle"
    PREPARING = "preparing"
    PUBLISHING = "publishing"
    RETRYING = "retrying"

    def __str__(self) -> str:
        return str(self.value)
