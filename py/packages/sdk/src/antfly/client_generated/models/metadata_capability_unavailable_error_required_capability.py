from enum import StrEnum


class MetadataCapabilityUnavailableErrorRequiredCapability(StrEnum):
    LINEARIZABLE_SNAPSHOT = "linearizable_snapshot"

    def __str__(self) -> str:
        return str(self.value)
