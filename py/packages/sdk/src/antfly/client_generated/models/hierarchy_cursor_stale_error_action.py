from enum import StrEnum


class HierarchyCursorStaleErrorAction(StrEnum):
    RESTART_HIERARCHY_TRAVERSAL = "restart_hierarchy_traversal"

    def __str__(self) -> str:
        return str(self.value)
