from enum import StrEnum


class MetadataLeaderUnavailableErrorError(StrEnum):
    METADATA_LEADER_UNAVAILABLE = "metadata leader unavailable"

    def __str__(self) -> str:
        return str(self.value)
