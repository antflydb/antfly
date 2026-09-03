from enum import Enum


class CohereEmbedderConfigProvider(str, Enum):
    COHERE = "cohere"

    def __str__(self) -> str:
        return str(self.value)
