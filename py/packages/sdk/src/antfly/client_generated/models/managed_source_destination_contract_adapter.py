from enum import StrEnum


class ManagedSourceDestinationContractAdapter(StrEnum):
    NATIVE = "native"
    WRITABLE_ICEBERG = "writable_iceberg"

    def __str__(self) -> str:
        return str(self.value)
