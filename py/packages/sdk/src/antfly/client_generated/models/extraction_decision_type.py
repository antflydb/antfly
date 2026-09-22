from enum import StrEnum


class ExtractionDecisionType(StrEnum):
    BOOLEAN = "boolean"
    CHOICE = "choice"
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
