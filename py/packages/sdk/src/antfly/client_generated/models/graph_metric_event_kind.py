from enum import Enum


class GraphMetricEventKind(str, Enum):
    DELETE = "delete"
    FAILED = "failed"
    PAUSE = "pause"
    PUBLISH = "publish"
    RESUME = "resume"

    def __str__(self) -> str:
        return str(self.value)
