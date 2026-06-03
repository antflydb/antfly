from enum import Enum


class InferenceToolChoiceType1Type(str, Enum):
    FUNCTION = "function"

    def __str__(self) -> str:
        return str(self.value)
