from enum import Enum


class UnsupportedHierarchyGroupingErrorField(str, Enum):
    HIERARCHY_GROUP_BY_LEVEL = "hierarchy.group_by.level"

    def __str__(self) -> str:
        return str(self.value)
