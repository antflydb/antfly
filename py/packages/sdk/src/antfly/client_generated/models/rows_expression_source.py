from enum import Enum


class RowsExpressionSource(str, Enum):
    EXISTING = "existing"
    PROPOSED = "proposed"
    ROW = "row"

    def __str__(self) -> str:
        return str(self.value)
