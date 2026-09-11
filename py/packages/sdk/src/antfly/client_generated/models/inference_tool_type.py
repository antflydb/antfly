from enum import StrEnum


class InferenceToolType(StrEnum):
    FUNCTION = "function"

    def __str__(self) -> str:
        return str(self.value)
