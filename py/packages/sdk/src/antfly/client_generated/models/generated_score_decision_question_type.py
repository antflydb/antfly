from enum import StrEnum


class GeneratedScoreDecisionQuestionType(StrEnum):
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
