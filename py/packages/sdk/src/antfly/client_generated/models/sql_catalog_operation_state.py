from enum import StrEnum


class SQLCatalogOperationState(StrEnum):
    COMMITTED = "committed"
    PENDING = "pending"
    REJECTED = "rejected"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
