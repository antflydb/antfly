from enum import StrEnum


class ManagedSourceDestinationContractInput(StrEnum):
    COMMITTED_SNAPSHOTS = "committed_snapshots"
    ROW_TRANSACTIONS = "row_transactions"
    UNAVAILABLE = "unavailable"

    def __str__(self) -> str:
        return str(self.value)
