from enum import Enum


class EmbeddingIndexActivityPhase(str, Enum):
    EMBEDDING = "embedding"
    IDLE = "idle"
    PREPARING = "preparing"
    PUBLISHING = "publishing"
    WAITING_RETRY = "waiting_retry"

    def __str__(self) -> str:
        return str(self.value)
