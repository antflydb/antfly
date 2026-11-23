from enum import StrEnum


class ManagedSourceDestinationContractTransactionScope(StrEnum):
    ICEBERG_TABLE = "iceberg_table"
    NATIVE_TRANSACTION = "native_transaction"
    NONE = "none"

    def __str__(self) -> str:
        return str(self.value)
