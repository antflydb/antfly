from enum import StrEnum


class GraphMetricEdgeFilterMode(StrEnum):
    ALL = "all"

    def __str__(self) -> str:
        return str(self.value)
