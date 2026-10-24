from enum import StrEnum


class MemoryEvaluationDatasetRequestVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
