from enum import Enum


class HierarchyCursorStaleErrorRestartWithout(str, Enum):
    SEARCH_AFTER = "search_after"

    def __str__(self) -> str:
        return str(self.value)
