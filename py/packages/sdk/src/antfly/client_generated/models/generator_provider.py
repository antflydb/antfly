from enum import StrEnum


class GeneratorProvider(StrEnum):
    ANTFLY = "antfly"
    ANTHROPIC = "anthropic"
    BEDROCK = "bedrock"
    COHERE = "cohere"
    GEMINI = "gemini"
    MOCK = "mock"
    OLLAMA = "ollama"
    OPENAI = "openai"
    OPENROUTER = "openrouter"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
