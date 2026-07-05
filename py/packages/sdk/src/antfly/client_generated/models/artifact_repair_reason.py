from enum import Enum


class ArtifactRepairReason(str, Enum):
    CORRUPT_ARTIFACT = "corrupt_artifact"
    MISSING_ARTIFACT = "missing_artifact"
    UNREADABLE_ARTIFACT = "unreadable_artifact"

    def __str__(self) -> str:
        return str(self.value)
