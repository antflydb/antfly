from enum import Enum


class InferenceRecognizeResponseObject(str, Enum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
