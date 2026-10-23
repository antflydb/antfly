from enum import StrEnum


class SQLTemporalKind(StrEnum):
    DATE = "date"
    INTERVAL = "interval"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"
    TIME_TZ = "time_tz"

    def __str__(self) -> str:
        return str(self.value)
