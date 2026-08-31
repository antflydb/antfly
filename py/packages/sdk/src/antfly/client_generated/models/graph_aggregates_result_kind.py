from enum import Enum


class GraphAggregatesResultKind(str, Enum):
    AGGREGATES = "aggregates"

    def __str__(self) -> str:
        return str(self.value)
