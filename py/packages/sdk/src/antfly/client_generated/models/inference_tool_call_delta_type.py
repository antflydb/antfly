from enum import StrEnum


class InferenceToolCallDeltaType(StrEnum):
    FUNCTION = "function"

    def __str__(self) -> str:
        return str(self.value)
