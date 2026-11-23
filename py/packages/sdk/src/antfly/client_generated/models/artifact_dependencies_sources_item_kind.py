from enum import StrEnum


class ArtifactDependenciesSourcesItemKind(StrEnum):
    DOCUMENT = "document"
    QUERY = "query"

    def __str__(self) -> str:
        return str(self.value)
