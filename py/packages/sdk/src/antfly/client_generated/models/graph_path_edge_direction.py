from enum import Enum


class GraphPathEdgeDirection(str, Enum):
    IN = "in"
    OUT = "out"

    def __str__(self) -> str:
        return str(self.value)
