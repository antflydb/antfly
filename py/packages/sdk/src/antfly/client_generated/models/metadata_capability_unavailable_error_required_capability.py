from enum import Enum


class MetadataCapabilityUnavailableErrorRequiredCapability(str, Enum):
    LINEARIZABLE_SNAPSHOT = "linearizable_snapshot"

    def __str__(self) -> str:
        return str(self.value)
