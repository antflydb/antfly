from enum import StrEnum


class GoogleGeneratorConfigProvider(StrEnum):
    GEMINI = "gemini"

    def __str__(self) -> str:
        return str(self.value)
