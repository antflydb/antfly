from enum import StrEnum


class JoinType(StrEnum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"

    def __str__(self) -> str:
        return str(self.value)
