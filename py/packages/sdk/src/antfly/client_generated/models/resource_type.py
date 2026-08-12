from enum import Enum


class ResourceType(str, Enum):
    INFERENCE = "inference"
    TABLE = "table"
    USER = "user"
    VALUE_3 = "*"

    def __str__(self) -> str:
        return str(self.value)
