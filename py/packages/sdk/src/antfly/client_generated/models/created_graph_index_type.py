from enum import Enum


class CreatedGraphIndexType(str, Enum):
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
