from enum import StrEnum


class SQLColumnType(StrEnum):
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    INTEGER = "integer"
    JSON = "json"
    NUMBER = "number"
    STRING = "string"
    UNKNOWN = "unknown"
    UUID = "uuid"

    def __str__(self) -> str:
        return str(self.value)
