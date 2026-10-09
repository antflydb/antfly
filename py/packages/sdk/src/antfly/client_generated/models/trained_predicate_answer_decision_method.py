from enum import StrEnum


class TrainedPredicateAnswerDecisionMethod(StrEnum):
    TYPED = "typed"

    def __str__(self) -> str:
        return str(self.value)
