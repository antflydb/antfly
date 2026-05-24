from enum import Enum


class TermiteRerankResponseObject(str, Enum):
    LIST = "list"

    def __str__(self) -> str:
        return str(self.value)
