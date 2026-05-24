from enum import Enum


class GraphIndexStatsIndexType(str, Enum):
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
