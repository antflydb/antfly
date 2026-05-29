from enum import Enum


class InferenceRecognizeObjectObject(str, Enum):
    RECOGNITION = "recognition"

    def __str__(self) -> str:
        return str(self.value)
