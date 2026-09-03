from enum import Enum


class ArtifactSourcesCapabilityState(str, Enum):
    AVAILABLE = "available"
    UNSUPPORTED = "unsupported"
    UPGRADE_PENDING = "upgrade_pending"

    def __str__(self) -> str:
        return str(self.value)
