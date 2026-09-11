from enum import StrEnum


class GraphAggregatesResultKind(StrEnum):
    AGGREGATES = "aggregates"

    def __str__(self) -> str:
        return str(self.value)
