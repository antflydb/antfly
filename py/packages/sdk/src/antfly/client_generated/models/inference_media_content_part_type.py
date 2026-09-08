from enum import StrEnum


class InferenceMediaContentPartType(StrEnum):
    MEDIA = "media"

    def __str__(self) -> str:
        return str(self.value)
