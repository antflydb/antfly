from enum import StrEnum


class InferenceReadResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
