from enum import StrEnum


class UnsupportedHierarchyGroupingErrorError(StrEnum):
    UNSUPPORTED_HIERARCHY_GROUPING = "unsupported_hierarchy_grouping"

    def __str__(self) -> str:
        return str(self.value)
