from enum import Enum


class QueryHierarchyRollup(str, Enum):
    NONE = "none"
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
