from enum import StrEnum


class GraphPathEdgeDirection(StrEnum):
    IN = "in"
    OUT = "out"

    def __str__(self) -> str:
        return str(self.value)
