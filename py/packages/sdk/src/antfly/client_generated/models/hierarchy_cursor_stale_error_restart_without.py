from enum import StrEnum


class HierarchyCursorStaleErrorRestartWithout(StrEnum):
    SEARCH_AFTER = "search_after"

    def __str__(self) -> str:
        return str(self.value)
