from enum import StrEnum


class EmbeddingExtractionDecisionMode(StrEnum):
    MULTI = "multi"
    SINGLE = "single"

    def __str__(self) -> str:
        return str(self.value)
