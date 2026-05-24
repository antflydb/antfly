from enum import Enum


class EdgeTypeConfigTopology(str, Enum):
    GRAPH = "graph"
    TREE = "tree"

    def __str__(self) -> str:
        return str(self.value)
