from enum import Enum


class GoogleEmbedderConfigProvider(str, Enum):
    GEMINI = "gemini"

    def __str__(self) -> str:
        return str(self.value)
