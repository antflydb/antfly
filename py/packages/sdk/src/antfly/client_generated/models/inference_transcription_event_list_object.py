from enum import StrEnum


class InferenceTranscriptionEventListObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
