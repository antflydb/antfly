from enum import StrEnum


class RerankerProvider(StrEnum):
    ANTFLY = "antfly"
    COHERE = "cohere"
    OLLAMA = "ollama"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
