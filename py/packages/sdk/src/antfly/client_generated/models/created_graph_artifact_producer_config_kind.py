from enum import Enum


class CreatedGraphArtifactProducerConfigKind(str, Enum):
    ASSET = "asset"

    def __str__(self) -> str:
        return str(self.value)
