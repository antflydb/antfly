from enum import Enum


class GraphArtifactSourceConfigKind(str, Enum):
    ARTIFACT = "artifact"

    def __str__(self) -> str:
        return str(self.value)
