from enum import Enum


class OpenAIGeneratorConfigProvider(str, Enum):
    OPENAI = "openai"

    def __str__(self) -> str:
        return str(self.value)
