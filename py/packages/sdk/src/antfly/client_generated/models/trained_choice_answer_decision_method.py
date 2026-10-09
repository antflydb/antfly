from enum import StrEnum


class TrainedChoiceAnswerDecisionMethod(StrEnum):
    TYPED = "typed"

    def __str__(self) -> str:
        return str(self.value)
