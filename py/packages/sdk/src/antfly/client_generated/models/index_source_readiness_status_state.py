from enum import StrEnum


class IndexSourceReadinessStatusState(StrEnum):
    FAILED = "failed"
    PENDING = "pending"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
