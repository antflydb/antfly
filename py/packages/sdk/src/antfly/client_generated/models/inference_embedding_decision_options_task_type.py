from enum import StrEnum


class InferenceEmbeddingDecisionOptionsTaskType(StrEnum):
    CLASSIFICATION = "CLASSIFICATION"
    CLUSTERING = "CLUSTERING"

    def __str__(self) -> str:
        return str(self.value)
