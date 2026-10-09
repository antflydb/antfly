from enum import StrEnum


class TrainedScoreAnswerDecisionMethod(StrEnum):
    TYPED = "typed"

    def __str__(self) -> str:
        return str(self.value)
