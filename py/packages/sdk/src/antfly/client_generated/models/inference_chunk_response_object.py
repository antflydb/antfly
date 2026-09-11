from enum import StrEnum


class InferenceChunkResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
