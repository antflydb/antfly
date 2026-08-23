from enum import Enum


class HierarchyGroupByLevel(str, Enum):
    SOURCE = "source"
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
