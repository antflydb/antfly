from enum import StrEnum


class MemoryEvaluationRunPhase(StrEnum):
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
