from enum import StrEnum


class OllamaGeneratorConfigProvider(StrEnum):
    OLLAMA = "ollama"

    def __str__(self) -> str:
        return str(self.value)
