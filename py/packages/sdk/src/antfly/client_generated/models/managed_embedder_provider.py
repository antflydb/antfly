from enum import Enum


class ManagedEmbedderProvider(str, Enum):
    ANTFLY = "antfly"
    BEDROCK = "bedrock"
    OLLAMA = "ollama"
    OPENAI = "openai"

    def __str__(self) -> str:
        return str(self.value)
