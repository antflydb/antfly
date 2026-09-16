from enum import StrEnum


class InferenceTranscriptionEventType(StrEnum):
    FINAL = "final"
    PARTIAL = "partial"

    def __str__(self) -> str:
        return str(self.value)
