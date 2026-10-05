from enum import StrEnum


class ExternalLakeSnapshotSelectorMode(StrEnum):
    CURRENT = "current"
    OBJECT_VERSION_DIGEST = "object_version_digest"
    SNAPSHOT_ID = "snapshot_id"

    def __str__(self) -> str:
        return str(self.value)
