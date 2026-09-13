from enum import StrEnum


class GraphMetricConfigRefresh(StrEnum):
    BACKGROUND = "background"
    MANUAL = "manual"

    def __str__(self) -> str:
        return str(self.value)
