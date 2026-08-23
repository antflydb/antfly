from enum import Enum


class GraphArtifactNodeMappingConfigModel(str, Enum):
    DOCUMENT = "document"
    EXTERNAL = "external"

    def __str__(self) -> str:
        return str(self.value)
