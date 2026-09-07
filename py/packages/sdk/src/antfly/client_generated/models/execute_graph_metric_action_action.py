from enum import StrEnum


class ExecuteGraphMetricActionAction(StrEnum):
    DELETE = "delete"
    PAUSE = "pause"
    REBUILD = "rebuild"
    REFRESH = "refresh"
    RESUME = "resume"

    def __str__(self) -> str:
        return str(self.value)
