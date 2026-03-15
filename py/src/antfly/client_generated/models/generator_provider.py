from enum import Enum


class GeneratorProvider(str, Enum):
    ANTHROPIC = "anthropic"
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
