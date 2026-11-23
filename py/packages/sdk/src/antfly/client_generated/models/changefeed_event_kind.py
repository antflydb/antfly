from enum import StrEnum


class ChangefeedEventKind(StrEnum):
    ARTIFACT_PUBLISHED = "artifact_published"
    DOCUMENT_COMMITTED = "document_committed"
    DOCUMENT_SNAPSHOT = "document_snapshot"
    PUBLICATION_ACTIVATED = "publication_activated"

    def __str__(self) -> str:
        return str(self.value)
