from enum import StrEnum


class OllamaEmbedderConfigProvider(StrEnum):
    OLLAMA = "ollama"

    def __str__(self) -> str:
        return str(self.value)
