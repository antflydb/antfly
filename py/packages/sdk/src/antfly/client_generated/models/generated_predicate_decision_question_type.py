from enum import StrEnum


class GeneratedPredicateDecisionQuestionType(StrEnum):
    PREDICATE = "predicate"

    def __str__(self) -> str:
        return str(self.value)
