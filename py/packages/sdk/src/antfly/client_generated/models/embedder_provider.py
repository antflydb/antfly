from enum import StrEnum


class EmbedderProvider(StrEnum):
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
