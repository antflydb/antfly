from enum import Enum


class QueryHierarchyReturnLevel(str, Enum):
    CHUNK = "chunk"
    MENTION = "mention"
    SOURCE = "source"
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
