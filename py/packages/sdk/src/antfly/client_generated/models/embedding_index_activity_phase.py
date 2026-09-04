from enum import StrEnum


class EmbeddingIndexActivityPhase(StrEnum):
    EMBEDDING = "embedding"
    IDLE = "idle"
    PREPARING = "preparing"
    PUBLISHING = "publishing"
    WAITING_RETRY = "waiting_retry"

    def __str__(self) -> str:
        return str(self.value)
