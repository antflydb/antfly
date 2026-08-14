from enum import Enum


class HierarchyChildrenLevel(str, Enum):
    CHUNK = "chunk"

    def __str__(self) -> str:
        return str(self.value)
