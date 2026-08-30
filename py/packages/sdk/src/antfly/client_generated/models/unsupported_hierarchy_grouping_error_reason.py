from enum import Enum


class UnsupportedHierarchyGroupingErrorReason(str, Enum):
    UNIT_IDENTITY_UNAVAILABLE = "unit_identity_unavailable"

    def __str__(self) -> str:
        return str(self.value)
