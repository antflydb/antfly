from enum import Enum


class RowsJoinRequestJoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"

    def __str__(self) -> str:
        return str(self.value)
