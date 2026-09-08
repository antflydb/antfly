from enum import StrEnum


class HierarchyChildParentLevel(StrEnum):
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
