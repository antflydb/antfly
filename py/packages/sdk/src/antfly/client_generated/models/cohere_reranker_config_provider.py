from enum import Enum


class CohereRerankerConfigProvider(str, Enum):
    COHERE = "cohere"

    def __str__(self) -> str:
        return str(self.value)
