from enum import StrEnum


class RelationalExpressionType(StrEnum):
    BLOB = "blob"
    BOOLEAN = "boolean"
    DATERANGE = "daterange"
    DATETIME = "datetime"
    INTEGER = "integer"
    JSON = "json"
    NUMBER = "number"
    NUMERIC = "numeric"
    NUMRANGE = "numrange"
    SQL_ARRAY = "sql_array"
    STRING = "string"
    TSRANGE = "tsrange"
    TSTZRANGE = "tstzrange"

    def __str__(self) -> str:
        return str(self.value)
