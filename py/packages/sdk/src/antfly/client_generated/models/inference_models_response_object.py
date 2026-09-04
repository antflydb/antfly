from enum import StrEnum


class InferenceModelsResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
