from enum import StrEnum


class InferenceChunkEmbeddingResponseStatus(StrEnum):
    COMPLETE = "complete"
    FAILED = "failed"
    PARTIAL = "partial"

    def __str__(self) -> str:
        return str(self.value)
