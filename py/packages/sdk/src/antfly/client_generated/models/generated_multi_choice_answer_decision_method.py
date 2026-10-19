from enum import StrEnum


class GeneratedMultiChoiceAnswerDecisionMethod(StrEnum):
    GENERATED = "generated"

    def __str__(self) -> str:
        return str(self.value)
