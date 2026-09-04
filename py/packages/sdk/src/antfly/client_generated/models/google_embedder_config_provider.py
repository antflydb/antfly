from enum import StrEnum


class GoogleEmbedderConfigProvider(StrEnum):
    GEMINI = "gemini"

    def __str__(self) -> str:
        return str(self.value)
