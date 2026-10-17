from enum import StrEnum


class ChunkSourceReferenceType(StrEnum):
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
