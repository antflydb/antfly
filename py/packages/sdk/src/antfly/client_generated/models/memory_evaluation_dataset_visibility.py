from enum import StrEnum


class MemoryEvaluationDatasetVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
