from enum import StrEnum


class ArtifactSourcesCapabilityState(StrEnum):
    AVAILABLE = "available"
    UNSUPPORTED = "unsupported"
    UPGRADE_PENDING = "upgrade_pending"

    def __str__(self) -> str:
        return str(self.value)
