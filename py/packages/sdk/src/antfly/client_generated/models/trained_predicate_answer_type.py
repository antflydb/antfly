from enum import StrEnum


class TrainedPredicateAnswerType(StrEnum):
    PREDICATE = "predicate"

    def __str__(self) -> str:
        return str(self.value)
