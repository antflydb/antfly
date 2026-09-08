from enum import StrEnum


class UnsupportedHierarchyGroupingErrorReason(StrEnum):
    UNIT_IDENTITY_UNAVAILABLE = "unit_identity_unavailable"

    def __str__(self) -> str:
        return str(self.value)
