from enum import StrEnum


class SQLColumnType(StrEnum):
    ARRAY = "array"
    BOOLEAN = "boolean"
    DATERANGE = "daterange"
    DATETIME = "datetime"
    INTEGER = "integer"
    JSON = "json"
    NUMBER = "number"
    NUMRANGE = "numrange"
    STRING = "string"
    TSRANGE = "tsrange"
    TSTZRANGE = "tstzrange"
    UNKNOWN = "unknown"
    UUID = "uuid"

    def __str__(self) -> str:
        return str(self.value)
