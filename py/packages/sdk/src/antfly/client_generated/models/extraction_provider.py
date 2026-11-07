from enum import StrEnum


class ExtractionProvider(StrEnum):
    ANTFLY = "antfly"
    ANTHROPIC = "anthropic"
    BEDROCK = "bedrock"
    CLOUDFLARE = "cloudflare"
    GEMINI = "gemini"
    MOCK = "mock"
    OPENAI = "openai"
    PIONEER = "pioneer"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
