from enum import StrEnum


class DocumentArtifactPublicationFailedErrorError(StrEnum):
    DOCUMENT_ARTIFACT_PUBLICATION_FAILED = "document_artifact_publication_failed"

    def __str__(self) -> str:
        return str(self.value)
