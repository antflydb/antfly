from enum import StrEnum


class GraphMetricEventKind(StrEnum):
    DELETE = "delete"
    FAILED = "failed"
    PAUSE = "pause"
    PUBLISH = "publish"
    RESUME = "resume"

    def __str__(self) -> str:
        return str(self.value)
