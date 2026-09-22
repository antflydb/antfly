from enum import StrEnum


class SQLTransactionStatus(StrEnum):
    FAILED = "failed"
    IDLE = "idle"
    IN_TRANSACTION = "in_transaction"

    def __str__(self) -> str:
        return str(self.value)
