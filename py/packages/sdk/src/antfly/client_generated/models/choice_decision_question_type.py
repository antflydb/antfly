from enum import StrEnum


class ChoiceDecisionQuestionType(StrEnum):
    CHOICE = "choice"

    def __str__(self) -> str:
        return str(self.value)
