from enum import StrEnum


class SQLDDLReceiptState(StrEnum):
    INVALID = "invalid"
    PENDING = "pending"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
