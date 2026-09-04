from enum import StrEnum


class GraphNodesResultKind(StrEnum):
    NODES = "nodes"

    def __str__(self) -> str:
        return str(self.value)
