from enum import StrEnum


class ExtensionScopeKind(StrEnum):
    CLUSTER = "cluster"
    EMBEDDED_DB = "embedded_db"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
