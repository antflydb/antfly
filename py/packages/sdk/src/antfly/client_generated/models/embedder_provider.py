from enum import Enum


class EmbedderProvider(str, Enum):
    ANTFLY = "antfly"
    BEDROCK = "bedrock"
    COHERE = "cohere"
    GEMINI = "gemini"
    OLLAMA = "ollama"
    OPENAI = "openai"
    OPENROUTER = "openrouter"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
