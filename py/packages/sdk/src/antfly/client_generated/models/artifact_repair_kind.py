from enum import StrEnum


class ArtifactRepairKind(StrEnum):
    ALGEBRAIC = "algebraic"
    ASSET = "asset"
    CHUNK = "chunk"
    EMBEDDING = "embedding"
    FULL_TEXT = "full_text"
    GRAPH = "graph"

    def __str__(self) -> str:
        return str(self.value)
