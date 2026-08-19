from enum import Enum


class MetadataCapabilityUnavailableErrorCode(str, Enum):
    METADATA_CAPABILITY_UNAVAILABLE = "metadata_capability_unavailable"

    def __str__(self) -> str:
        return str(self.value)
