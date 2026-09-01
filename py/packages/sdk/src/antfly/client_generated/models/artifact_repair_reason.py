from enum import Enum


class ArtifactRepairReason(str, Enum):
    CORRUPT_ARTIFACT = "corrupt_artifact"
    ENRICHMENT_FAILED = "enrichment_failed"
    MISSING_ARTIFACT = "missing_artifact"
    RESOURCE_LIMIT_EXCEEDED = "resource_limit_exceeded"
    UNREADABLE_ARTIFACT = "unreadable_artifact"

    def __str__(self) -> str:
        return str(self.value)
