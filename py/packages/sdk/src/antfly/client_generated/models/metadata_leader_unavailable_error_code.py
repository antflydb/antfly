from enum import StrEnum


class MetadataLeaderUnavailableErrorCode(StrEnum):
    METADATA_LEADER_UNAVAILABLE = "metadata_leader_unavailable"

    def __str__(self) -> str:
        return str(self.value)
