from enum import Enum


class TermiteToolChoiceType1Type(str, Enum):
    FUNCTION = "function"

    def __str__(self) -> str:
        return str(self.value)
