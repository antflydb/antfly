from enum import StrEnum


class GraphArtifactProducerConfigKind(StrEnum):
    ASSET = "asset"

    def __str__(self) -> str:
        return str(self.value)
