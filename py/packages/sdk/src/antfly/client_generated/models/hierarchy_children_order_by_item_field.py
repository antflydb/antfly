from enum import StrEnum


class HierarchyChildrenOrderByItemField(StrEnum):
    VALUE_0 = "_hierarchy.position"

    def __str__(self) -> str:
        return str(self.value)
