from enum import Enum


class ResourceType(str, Enum):
    TABLE = "table"
    USER = "user"
    VALUE_2 = "*"

    def __str__(self) -> str:
        return str(self.value)
