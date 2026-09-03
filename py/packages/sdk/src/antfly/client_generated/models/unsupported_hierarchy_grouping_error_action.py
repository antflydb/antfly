from enum import Enum


class UnsupportedHierarchyGroupingErrorAction(str, Enum):
    USE_SOURCE_GROUPING_OR_DIRECT_MEMBERS = "use_source_grouping_or_direct_members"

    def __str__(self) -> str:
        return str(self.value)
