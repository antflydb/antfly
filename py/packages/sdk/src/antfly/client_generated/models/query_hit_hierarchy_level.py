from enum import Enum


class QueryHitHierarchyLevel(str, Enum):
    ARTIFACT = "artifact"
    CHUNK = "chunk"
    EMBEDDING = "embedding"
    MENTION = "mention"
    SOURCE = "source"
    UNIT = "unit"

    def __str__(self) -> str:
        return str(self.value)
