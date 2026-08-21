from enum import Enum


class HierarchyChildrenLevel(str, Enum):
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
