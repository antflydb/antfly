from enum import StrEnum


class EmbeddingExtractionDecisionStatus(StrEnum):
    ABSTAINED = "abstained"
    SELECTED = "selected"

    def __str__(self) -> str:
        return str(self.value)
