from enum import StrEnum


class ResourceType(StrEnum):
    DATABASE = "database"
    DATASET = "dataset"
    GRAPH = "graph"
    INFERENCE = "inference"
    NAMESPACE = "namespace"
    TABLE = "table"
    TABLESPACE = "tablespace"
    USER = "user"
    VALUE_8 = "*"

    def __str__(self) -> str:
        return str(self.value)
