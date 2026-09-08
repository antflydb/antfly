from enum import StrEnum


class HierarchyArtifactSourceKind(StrEnum):
    ASSET = "asset"
    CHUNK = "chunk"
    EMBEDDING = "embedding"

    def __str__(self) -> str:
        return str(self.value)
