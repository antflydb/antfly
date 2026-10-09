from enum import StrEnum


class ScoreDecisionQuestionType(StrEnum):
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
