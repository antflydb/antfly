from enum import StrEnum


class InferenceTranscribeResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
