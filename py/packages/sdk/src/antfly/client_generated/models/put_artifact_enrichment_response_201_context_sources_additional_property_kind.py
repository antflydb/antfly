from enum import StrEnum


class PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind(StrEnum):
    DOCUMENT = "document"
    QUERY = "query"

    def __str__(self) -> str:
        return str(self.value)
