from enum import StrEnum


class GeneratorProvider(StrEnum):
    ANTFLY = "antfly"
    GEMINI = "gemini"
    OLLAMA = "ollama"
    OPENAI = "openai"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
