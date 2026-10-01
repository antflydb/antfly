from enum import StrEnum


class OpenRouterGeneratorConfigProvider(StrEnum):
    OPENROUTER = "openrouter"

    def __str__(self) -> str:
        return str(self.value)
