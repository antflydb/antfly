from enum import StrEnum


class MediaContentPartType(StrEnum):
    MEDIA = "media"

    def __str__(self) -> str:
        return str(self.value)
