from enum import Enum


class MetadataCapabilityUnavailableErrorError(str, Enum):
    METADATA_CAPABILITY_UNAVAILABLE = "metadata capability unavailable"

    def __str__(self) -> str:
        return str(self.value)
