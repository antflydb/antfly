from enum import StrEnum


class GeneratedScoreAnswerType(StrEnum):
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
