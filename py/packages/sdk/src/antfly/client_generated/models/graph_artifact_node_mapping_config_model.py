from enum import StrEnum


class GraphArtifactNodeMappingConfigModel(StrEnum):
    DOCUMENT = "document"
    EXTERNAL = "external"

    def __str__(self) -> str:
        return str(self.value)
