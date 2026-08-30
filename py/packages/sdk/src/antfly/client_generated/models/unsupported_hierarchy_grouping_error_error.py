from enum import Enum


class UnsupportedHierarchyGroupingErrorError(str, Enum):
    UNSUPPORTED_HIERARCHY_GROUPING = "unsupported_hierarchy_grouping"

    def __str__(self) -> str:
        return str(self.value)
