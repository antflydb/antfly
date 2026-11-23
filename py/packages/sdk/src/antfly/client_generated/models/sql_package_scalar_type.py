from enum import StrEnum


class SQLPackageScalarType(StrEnum):
    BOOLEAN = "boolean"
    DATE = "date"
    DATERANGE = "daterange"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    INTERVAL = "interval"
    JSONB = "jsonb"
    NUMERIC = "numeric"
    NUMRANGE = "numrange"
    TEXT = "text"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"
    TIME_TZ = "time_tz"
    TSRANGE = "tsrange"
    TSTZRANGE = "tstzrange"
    UUID = "uuid"

    def __str__(self) -> str:
        return str(self.value)
