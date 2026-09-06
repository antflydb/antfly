from enum import StrEnum


class EnrichmentRuntimeStatusStallReason(StrEnum):
    EMBEDDING_OVERDUE = "embedding_overdue"
    MODEL_LOADING = "model_loading"
    PUBLISHING_OVERDUE = "publishing_overdue"
    VALUE_0 = ""
    WORKER_MISSING = "worker_missing"

    def __str__(self) -> str:
        return str(self.value)
