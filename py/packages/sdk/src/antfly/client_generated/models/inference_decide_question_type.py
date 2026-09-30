from enum import StrEnum


class InferenceDecideQuestionType(StrEnum):
    CHOICE = "choice"
    NOUL = "noul"
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
