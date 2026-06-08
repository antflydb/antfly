from enum import Enum


class GraphMetricEdgeFilterStatusMode(str, Enum):
    ALL = "all"
    TYPES = "types"

    def __str__(self) -> str:
        return str(self.value)
