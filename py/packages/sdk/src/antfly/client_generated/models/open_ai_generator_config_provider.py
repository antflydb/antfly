from enum import StrEnum


class OpenAIGeneratorConfigProvider(StrEnum):
    OPENAI = "openai"

    def __str__(self) -> str:
        return str(self.value)
