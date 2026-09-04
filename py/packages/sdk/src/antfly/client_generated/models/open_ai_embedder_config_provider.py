from enum import StrEnum


class OpenAIEmbedderConfigProvider(StrEnum):
    OPENAI = "openai"

    def __str__(self) -> str:
        return str(self.value)
