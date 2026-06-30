from enum import Enum


class RowOperationOp(str, Enum):
    DELETE = "delete"
    INSERT = "insert"
    UPDATE = "update"
    UPSERT = "upsert"

    def __str__(self) -> str:
        return str(self.value)
