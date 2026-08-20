from enum import Enum


class ExecuteGraphMetricActionAction(str, Enum):
    DELETE = "delete"
    PAUSE = "pause"
    REBUILD = "rebuild"
    REFRESH = "refresh"
    RESUME = "resume"

    def __str__(self) -> str:
        return str(self.value)
