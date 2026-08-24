from enum import Enum


class IndexReadinessState(str, Enum):
    FAILED = "failed"
    PENDING = "pending"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
