from enum import Enum


class RerankerProvider(str, Enum):
    ANTFLY = "antfly"
    COHERE = "cohere"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
