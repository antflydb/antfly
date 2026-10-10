from enum import StrEnum


class SQLArrayElementType(StrEnum):
    BOOLEAN = "boolean"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    JSONB = "jsonb"
    NUMERIC = "numeric"
    TEXT = "text"
    UUID = "uuid"

    def __str__(self) -> str:
        return str(self.value)
