from enum import Enum


class RowsExpressionFieldSource(str, Enum):
    EXISTING = "existing"
    PROPOSED = "proposed"
    ROW = "row"
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
