from enum import StrEnum


class GraphResolverConfigSourceArtifactKind(StrEnum):
    ANY = "any"
    ASSET = "asset"
    CHUNK = "chunk"

    def __str__(self) -> str:
        return str(self.value)
