from enum import StrEnum


class UnsupportedHierarchyGroupingErrorAction(StrEnum):
    USE_SOURCE_GROUPING_OR_DIRECT_MEMBERS = "use_source_grouping_or_direct_members"

    def __str__(self) -> str:
        return str(self.value)
