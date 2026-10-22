from enum import StrEnum


class GeneratedPredicateAnswerDecisionMethod(StrEnum):
    GENERATED = "generated"

    def __str__(self) -> str:
        return str(self.value)
