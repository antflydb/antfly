from enum import StrEnum


class UnsupportedHierarchyGroupingErrorField(StrEnum):
    HIERARCHY_GROUP_BY_LEVEL = "hierarchy.group_by.level"

    def __str__(self) -> str:
        return str(self.value)
