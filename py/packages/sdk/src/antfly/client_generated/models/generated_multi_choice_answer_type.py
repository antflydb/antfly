from enum import StrEnum


class GeneratedMultiChoiceAnswerType(StrEnum):
    MULTI_CHOICE = "multi_choice"

    def __str__(self) -> str:
        return str(self.value)
