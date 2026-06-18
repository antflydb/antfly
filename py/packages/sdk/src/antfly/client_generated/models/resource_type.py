from enum import Enum


class ResourceType(str, Enum):
    DATABASE = "database"
    NAMESPACE = "namespace"
    TABLE = "table"
    TABLESPACE = "tablespace"
    USER = "user"
    VALUE_5 = "*"

    def __str__(self) -> str:
        return str(self.value)
