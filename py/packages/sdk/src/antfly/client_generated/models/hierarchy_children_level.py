from enum import StrEnum


class HierarchyChildrenLevel(StrEnum):
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
