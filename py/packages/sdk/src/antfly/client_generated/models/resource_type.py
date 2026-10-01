from enum import StrEnum


class ResourceType(StrEnum):
    DATABASE = "database"
    INFERENCE = "inference"
    NAMESPACE = "namespace"
    TABLE = "table"
    TABLESPACE = "tablespace"
    USER = "user"
    VALUE_6 = "*"

    def __str__(self) -> str:
        return str(self.value)
