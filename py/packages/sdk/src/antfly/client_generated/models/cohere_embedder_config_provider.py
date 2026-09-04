from enum import StrEnum


class CohereEmbedderConfigProvider(StrEnum):
    COHERE = "cohere"

    def __str__(self) -> str:
        return str(self.value)
