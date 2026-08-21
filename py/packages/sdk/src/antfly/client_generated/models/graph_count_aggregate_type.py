from enum import Enum


class GraphCountAggregateType(str, Enum):
    COUNT = "count"

    def __str__(self) -> str:
        return str(self.value)
