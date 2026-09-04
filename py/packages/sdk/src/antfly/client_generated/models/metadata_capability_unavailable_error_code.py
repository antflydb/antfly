from enum import StrEnum


class MetadataCapabilityUnavailableErrorCode(StrEnum):
    METADATA_CAPABILITY_UNAVAILABLE = "metadata_capability_unavailable"

    def __str__(self) -> str:
        return str(self.value)
