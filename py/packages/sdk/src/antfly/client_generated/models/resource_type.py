from enum import StrEnum


class ResourceType(StrEnum):
    INFERENCE = "inference"
    TABLE = "table"
    USER = "user"
    VALUE_3 = "*"

    def __str__(self) -> str:
        return str(self.value)
