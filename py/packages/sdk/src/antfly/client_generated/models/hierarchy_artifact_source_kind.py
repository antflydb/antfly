from enum import Enum


class HierarchyArtifactSourceKind(str, Enum):
    ASSET = "asset"
    CHUNK = "chunk"
    EMBEDDING = "embedding"

    def __str__(self) -> str:
        return str(self.value)
