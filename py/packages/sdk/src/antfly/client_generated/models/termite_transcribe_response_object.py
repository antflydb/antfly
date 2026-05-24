from enum import Enum


class TermiteTranscribeResponseObject(str, Enum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
