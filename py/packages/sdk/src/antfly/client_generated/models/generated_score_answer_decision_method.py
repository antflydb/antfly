from enum import StrEnum


class GeneratedScoreAnswerDecisionMethod(StrEnum):
    GENERATED = "generated"

    def __str__(self) -> str:
        return str(self.value)
