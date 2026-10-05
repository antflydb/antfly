from enum import StrEnum


class GeneratorProvider(StrEnum):
    ANTFLY = "antfly"
    APPLE = "apple"
    GEMINI = "gemini"
    OLLAMA = "ollama"
    OPENAI = "openai"
    OPENROUTER = "openrouter"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
