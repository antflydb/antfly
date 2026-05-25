from enum import Enum


class EdgeDirection(str, Enum):
    BOTH = "both"
    IN = "in"
    OUT = "out"

    def __str__(self) -> str:
        return str(self.value)
