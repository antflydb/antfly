from enum import Enum


class EmbedderProvider(str, Enum):
    ANTFLY = "antfly"
    BEDROCK = "bedrock"
    COHERE = "cohere"
    GEMINI = "gemini"
    MOCK = "mock"
    OLLAMA = "ollama"
    OPENAI = "openai"
    OPENROUTER = "openrouter"
    TERMITE = "termite"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
