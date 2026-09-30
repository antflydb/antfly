from enum import StrEnum


class SQLDDLReceiptState(StrEnum):
    ADMISSION_UNKNOWN = "admission_unknown"
    INVALID = "invalid"
    PENDING = "pending"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
