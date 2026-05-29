from enum import Enum


class InferenceChunkResponseObject(str, Enum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
