from enum import StrEnum


class InferenceModelInstanceListObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
