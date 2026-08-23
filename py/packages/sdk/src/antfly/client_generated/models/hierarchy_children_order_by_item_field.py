from enum import Enum


class HierarchyChildrenOrderByItemField(str, Enum):
    VALUE_0 = "_hierarchy.position"

    def __str__(self) -> str:
        return str(self.value)
