from enum import Enum


class RowsWindowFrameEnd(str, Enum):
    CURRENT_ROW = "current_row"
    OFFSET_FOLLOWING = "offset_following"
    OFFSET_PRECEDING = "offset_preceding"
    UNBOUNDED_FOLLOWING = "unbounded_following"

    def __str__(self) -> str:
        return str(self.value)
