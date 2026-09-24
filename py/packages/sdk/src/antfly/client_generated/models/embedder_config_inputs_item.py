from enum import StrEnum


class EmbedderConfigInputsItem(StrEnum):
    AUDIO = "audio"
    IMAGE = "image"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
