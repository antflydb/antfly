from enum import StrEnum


class ExtractionOptionsEmbeddingTaskType(StrEnum):
    CLASSIFICATION = "CLASSIFICATION"
    CLUSTERING = "CLUSTERING"

    def __str__(self) -> str:
        return str(self.value)
