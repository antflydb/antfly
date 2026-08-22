from enum import Enum


class GraphResolverConfigSourceArtifactKind(str, Enum):
    ANY = "any"
    ASSET = "asset"
    CHUNK = "chunk"

    def __str__(self) -> str:
        return str(self.value)
