from enum import StrEnum


class ManagedSourceDestinationContractCommittedHistory(StrEnum):
    ICEBERG_COMMITTED_WAL = "iceberg_committed_wal"
    NATIVE_ROWS_WHEN_ENROLLED = "native_rows_when_enrolled"
    UNAVAILABLE = "unavailable"

    def __str__(self) -> str:
        return str(self.value)
