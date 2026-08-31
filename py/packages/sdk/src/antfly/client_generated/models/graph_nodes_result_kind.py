from enum import Enum


class GraphNodesResultKind(str, Enum):
    NODES = "nodes"

    def __str__(self) -> str:
        return str(self.value)
