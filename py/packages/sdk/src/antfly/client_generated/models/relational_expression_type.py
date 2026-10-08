from enum import StrEnum


class RelationalExpressionType(StrEnum):
    BLOB = "blob"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    INTEGER = "integer"
    NUMBER = "number"
    NUMERIC = "numeric"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
