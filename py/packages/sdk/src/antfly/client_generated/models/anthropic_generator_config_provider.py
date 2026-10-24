from enum import StrEnum


class AnthropicGeneratorConfigProvider(StrEnum):
    ANTHROPIC = "anthropic"

    def __str__(self) -> str:
        return str(self.value)
