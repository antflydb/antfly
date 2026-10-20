from enum import StrEnum


class GeneratedChoiceDecisionQuestionType(StrEnum):
    CHOICE = "choice"

    def __str__(self) -> str:
        return str(self.value)
