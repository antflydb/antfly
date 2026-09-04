from enum import StrEnum


class GraphIndexStatsIndexType(StrEnum):
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
