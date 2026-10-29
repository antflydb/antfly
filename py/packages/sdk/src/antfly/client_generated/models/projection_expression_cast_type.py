from enum import StrEnum


class ProjectionExpressionCastType(StrEnum):
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    INTEGER = "integer"
    JSON = "json"
    NUMBER = "number"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
