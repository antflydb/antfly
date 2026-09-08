from enum import StrEnum


class MetadataCapabilityUnavailableErrorError(StrEnum):
    METADATA_CAPABILITY_UNAVAILABLE = "metadata capability unavailable"

    def __str__(self) -> str:
        return str(self.value)
