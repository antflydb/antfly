from enum import Enum


class MetadataLeaderUnavailableErrorCode(str, Enum):
    METADATA_LEADER_UNAVAILABLE = "metadata_leader_unavailable"

    def __str__(self) -> str:
        return str(self.value)
