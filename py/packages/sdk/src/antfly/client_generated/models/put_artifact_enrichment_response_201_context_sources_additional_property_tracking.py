from enum import StrEnum


class PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyTracking(StrEnum):
    LIVE = "live"
    SNAPSHOT = "snapshot"

    def __str__(self) -> str:
        return str(self.value)
