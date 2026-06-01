from enum import Enum


class RerankerProvider(str, Enum):
    ANTFLY = "antfly"
    COHERE = "cohere"
    OLLAMA = "ollama"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
