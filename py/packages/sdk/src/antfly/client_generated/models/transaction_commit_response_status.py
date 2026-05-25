from enum import Enum


class TransactionCommitResponseStatus(str, Enum):
    ABORTED = "aborted"
    COMMITTED = "committed"

    def __str__(self) -> str:
        return str(self.value)
