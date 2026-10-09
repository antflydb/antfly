from enum import StrEnum


class PredicateDecisionQuestionType(StrEnum):
    PREDICATE = "predicate"

    def __str__(self) -> str:
        return str(self.value)
