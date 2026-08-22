from enum import Enum


class CreateGraphIndexRequestType(str, Enum):
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
