from enum import Enum


class RowsMutationSourceRequestOp(str, Enum):
    DELETE = "delete"
    UPDATE = "update"

    def __str__(self) -> str:
        return str(self.value)
