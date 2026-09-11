from enum import StrEnum


class HierarchyCursorStaleErrorError(StrEnum):
    HIERARCHY_CURSOR_STALE = "hierarchy_cursor_stale"

    def __str__(self) -> str:
        return str(self.value)
