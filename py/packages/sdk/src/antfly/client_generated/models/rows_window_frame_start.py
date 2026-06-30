from enum import Enum


class RowsWindowFrameStart(str, Enum):
    CURRENT_ROW = "current_row"
    OFFSET_FOLLOWING = "offset_following"
    OFFSET_PRECEDING = "offset_preceding"
    UNBOUNDED_PRECEDING = "unbounded_preceding"

    def __str__(self) -> str:
        return str(self.value)
