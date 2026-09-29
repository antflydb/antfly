from enum import StrEnum


class InferenceDecideAnswerType(StrEnum):
    CHOICE = "choice"
    NOUL = "noul"
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
