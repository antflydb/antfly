from enum import StrEnum


class ResourceType(StrEnum):
    DATABASE = "database"
    GRAPH = "graph"
    INFERENCE = "inference"
    NAMESPACE = "namespace"
    TABLE = "table"
    TABLESPACE = "tablespace"
    USER = "user"
    VALUE_7 = "*"

    def __str__(self) -> str:
        return str(self.value)
