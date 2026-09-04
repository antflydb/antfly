from enum import StrEnum


class EdgeTypeConfigTopology(StrEnum):
    GRAPH = "graph"
    TREE = "tree"

    def __str__(self) -> str:
        return str(self.value)
