from enum import Enum


class RowsExpressionTo(str, Enum):
    BOOL = "bool"
    BOOLEAN = "boolean"
    NUMERIC = "numeric"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
