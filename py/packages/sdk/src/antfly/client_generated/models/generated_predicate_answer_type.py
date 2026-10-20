from enum import StrEnum


class GeneratedPredicateAnswerType(StrEnum):
    PREDICATE = "predicate"

    def __str__(self) -> str:
        return str(self.value)
