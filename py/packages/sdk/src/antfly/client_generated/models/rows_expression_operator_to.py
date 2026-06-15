from enum import Enum


class RowsExpressionOperatorTo(str, Enum):
    BOOL = "bool"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    NUMERIC = "numeric"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
