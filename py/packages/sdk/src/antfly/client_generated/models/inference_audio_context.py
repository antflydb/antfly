from enum import StrEnum


class InferenceAudioContext(StrEnum):
    DYNAMIC = "dynamic"
    FULL = "full"

    def __str__(self) -> str:
        return str(self.value)
