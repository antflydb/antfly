from enum import StrEnum


class ChunkCoordinatesKind(StrEnum):
    AUDIO = "audio"
    GIF = "gif"
    IMAGE = "image"
    PDF = "pdf"
    TEXT = "text"
    VIDEO = "video"

    def __str__(self) -> str:
        return str(self.value)
