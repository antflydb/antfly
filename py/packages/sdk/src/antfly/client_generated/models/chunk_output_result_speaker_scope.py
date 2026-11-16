from enum import StrEnum


class ChunkOutputResultSpeakerScope(StrEnum):
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
