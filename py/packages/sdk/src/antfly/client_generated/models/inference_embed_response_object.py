from enum import StrEnum


class InferenceEmbedResponseObject(StrEnum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
