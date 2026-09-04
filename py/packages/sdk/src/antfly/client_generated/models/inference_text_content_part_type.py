from enum import StrEnum


class InferenceTextContentPartType(StrEnum):
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
