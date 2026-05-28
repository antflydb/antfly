from enum import Enum


class InferenceRerankResponseObject(str, Enum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
