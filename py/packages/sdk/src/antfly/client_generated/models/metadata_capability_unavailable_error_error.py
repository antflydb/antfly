from enum import Enum


class MetadataCapabilityUnavailableErrorError(str, Enum):
    METADATA_CAPABILITY_UNAVAILABLE = "metadata_capability_unavailable"

    def __str__(self) -> str:
        return str(self.value)
