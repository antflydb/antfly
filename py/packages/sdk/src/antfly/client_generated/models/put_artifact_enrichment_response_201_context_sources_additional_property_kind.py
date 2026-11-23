from enum import StrEnum


class PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind(StrEnum):
    DOCUMENT = "document"
    GRAPH = "graph"
    QUERY = "query"

    def __str__(self) -> str:
        return str(self.value)
