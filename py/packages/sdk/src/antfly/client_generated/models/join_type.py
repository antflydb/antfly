from enum import Enum


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"

    def __str__(self) -> str:
        return str(self.value)
