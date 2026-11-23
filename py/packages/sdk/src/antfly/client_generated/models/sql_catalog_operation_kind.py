from enum import StrEnum


class SQLCatalogOperationKind(StrEnum):
    ALTER = "alter"
    CREATE = "create"
    DROP = "drop"
    RENAME = "rename"

    def __str__(self) -> str:
        return str(self.value)
