from enum import StrEnum


class CreatedGraphArtifactProducerConfigKind(StrEnum):
    ASSET = "asset"

    def __str__(self) -> str:
        return str(self.value)
