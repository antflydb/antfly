from enum import StrEnum


class CohereRerankerConfigProvider(StrEnum):
    COHERE = "cohere"

    def __str__(self) -> str:
        return str(self.value)
