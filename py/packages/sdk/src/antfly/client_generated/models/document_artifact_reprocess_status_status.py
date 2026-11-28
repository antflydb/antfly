from enum import StrEnum


class DocumentArtifactReprocessStatusStatus(StrEnum):
    PENDING = "pending"
    PUBLISHED = "published"
    SUPERSEDED = "superseded"
    UNVERIFIED = "unverified"

    def __str__(self) -> str:
        return str(self.value)
