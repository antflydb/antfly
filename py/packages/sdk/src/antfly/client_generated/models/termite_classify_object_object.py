from enum import Enum


class TermiteClassifyObjectObject(str, Enum):
    CLASSIFICATION = "classification"

    def __str__(self) -> str:
        return str(self.value)
