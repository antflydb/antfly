from enum import StrEnum


class ArtifactDependenciesSourcesItemKind(StrEnum):
    DOCUMENT = "document"
    GRAPH = "graph"
    QUERY = "query"

    def __str__(self) -> str:
        return str(self.value)
