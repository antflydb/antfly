from enum import Enum


class QueryHierarchyIncludeItem(str, Enum):
    CHUNK = "chunk"
    CHUNKS = "chunks"
    MENTION = "mention"
    MENTIONS = "mentions"
    SOURCE = "source"
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
