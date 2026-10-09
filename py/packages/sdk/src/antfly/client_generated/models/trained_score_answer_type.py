from enum import StrEnum


class TrainedScoreAnswerType(StrEnum):
    SCORE = "score"

    def __str__(self) -> str:
        return str(self.value)
