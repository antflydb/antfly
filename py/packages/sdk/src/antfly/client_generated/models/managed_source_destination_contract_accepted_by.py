from enum import StrEnum


class ManagedSourceDestinationContractAcceptedBy(StrEnum):
    ICEBERG_INGRESS = "iceberg_ingress"
    NATIVE_TRANSACTION = "native_transaction"
    NONE = "none"

    def __str__(self) -> str:
        return str(self.value)
