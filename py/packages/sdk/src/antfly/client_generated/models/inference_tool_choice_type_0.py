from enum import Enum


class InferenceToolChoiceType0(str, Enum):
    AUTO = "auto"
    NONE = "none"
    REQUIRED = "required"

    def __str__(self) -> str:
        return str(self.value)
