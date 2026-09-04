from enum import StrEnum


class HierarchyGroupByLevel(StrEnum):
    SOURCE = "source"
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
