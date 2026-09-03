from enum import Enum


class GoogleGeneratorConfigProvider(str, Enum):
    GEMINI = "gemini"

    def __str__(self) -> str:
        return str(self.value)
