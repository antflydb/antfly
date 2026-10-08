from enum import StrEnum


class InferenceDecideAnswerAbstentionReason(StrEnum):
    MIN_MARGIN = "min_margin"
    MIN_SIMILARITY = "min_similarity"
    TIE = "tie"

    def __str__(self) -> str:
        return str(self.value)
