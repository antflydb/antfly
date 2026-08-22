from enum import Enum


class MetadataLeaderUnavailableErrorError(str, Enum):
    METADATA_LEADER_UNAVAILABLE = "metadata leader unavailable"

    def __str__(self) -> str:
        return str(self.value)
