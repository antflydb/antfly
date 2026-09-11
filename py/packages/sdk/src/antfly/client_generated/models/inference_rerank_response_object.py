from enum import StrEnum


class InferenceRerankResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
