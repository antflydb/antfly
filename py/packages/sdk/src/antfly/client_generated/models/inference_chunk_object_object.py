from enum import StrEnum


class InferenceChunkObjectObject(StrEnum):
    CHUNK = "chunk"

    def __str__(self) -> str:
        return str(self.value)
