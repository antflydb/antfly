from enum import StrEnum


class InferenceToolChoiceType1Type(StrEnum):
    FUNCTION = "function"

    def __str__(self) -> str:
        return str(self.value)
