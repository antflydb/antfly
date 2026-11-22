from enum import StrEnum


class ManagedSourceDestinationContractCommittedBy(StrEnum):
    EXTERNAL_SNAPSHOT = "external_snapshot"
    ICEBERG_CATALOG_SNAPSHOT = "iceberg_catalog_snapshot"
    NATIVE_TRANSACTION = "native_transaction"
    NONE = "none"

    def __str__(self) -> str:
        return str(self.value)
