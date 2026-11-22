from enum import StrEnum


class ChangefeedCreateEventsItem(StrEnum):
    ARTIFACT_PUBLISHED = "artifact_published"
    DOCUMENT_COMMITTED = "document_committed"
    PUBLICATION_ACTIVATED = "publication_activated"

    def __str__(self) -> str:
        return str(self.value)
