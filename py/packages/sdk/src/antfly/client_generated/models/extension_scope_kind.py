from enum import Enum


class ExtensionScopeKind(str, Enum):
    CLUSTER = "cluster"
    EMBEDDED_DB = "embedded_db"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
