from enum import Enum


class HierarchyChildParentLevel(str, Enum):
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
