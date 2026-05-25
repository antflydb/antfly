from enum import Enum


class TermiteRecognizeObjectObject(str, Enum):
    RECOGNITION = "recognition"

    def __str__(self) -> str:
        return str(self.value)
