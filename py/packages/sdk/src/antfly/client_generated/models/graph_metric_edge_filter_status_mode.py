from enum import StrEnum


class GraphMetricEdgeFilterStatusMode(StrEnum):
    ALL = "all"
    TYPES = "types"

    def __str__(self) -> str:
        return str(self.value)
