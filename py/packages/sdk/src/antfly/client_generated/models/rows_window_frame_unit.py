from enum import Enum


class RowsWindowFrameUnit(str, Enum):
    RANGE = "range"
    ROWS = "rows"

    def __str__(self) -> str:
        return str(self.value)
