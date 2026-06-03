from enum import Enum


class InferenceChunkObjectObject(str, Enum):
    CHUNK = "chunk"

    def __str__(self) -> str:
        return str(self.value)
