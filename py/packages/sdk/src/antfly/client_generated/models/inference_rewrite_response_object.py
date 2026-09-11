from enum import StrEnum


class InferenceRewriteResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
