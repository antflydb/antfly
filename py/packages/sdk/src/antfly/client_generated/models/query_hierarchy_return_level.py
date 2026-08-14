from enum import Enum


class QueryHierarchyReturnLevel(str, Enum):
    CHUNK = "chunk"
    SOURCE = "source"

    def __str__(self) -> str:
        return str(self.value)
