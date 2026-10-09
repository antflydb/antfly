from enum import StrEnum


class EmbeddingDecisionOptionsTaskType(StrEnum):
    CLASSIFICATION = "CLASSIFICATION"
    CLUSTERING = "CLUSTERING"

    def __str__(self) -> str:
        return str(self.value)
