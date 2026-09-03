from enum import Enum


class OpenRouterEmbedderConfigProvider(str, Enum):
    OPENROUTER = "openrouter"

    def __str__(self) -> str:
        return str(self.value)
