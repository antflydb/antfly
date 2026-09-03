from enum import Enum


class GeneratorProvider(str, Enum):
    ANTFLY = "antfly"
    GEMINI = "gemini"
    OLLAMA = "ollama"
    OPENAI = "openai"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
