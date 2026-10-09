from enum import StrEnum


class MultiChoiceDecisionQuestionType(StrEnum):
    MULTI_CHOICE = "multi_choice"

    def __str__(self) -> str:
        return str(self.value)
