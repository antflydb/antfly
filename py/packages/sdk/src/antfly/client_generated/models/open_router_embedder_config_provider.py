from enum import StrEnum


class OpenRouterEmbedderConfigProvider(StrEnum):
    OPENROUTER = "openrouter"

    def __str__(self) -> str:
        return str(self.value)
