from enum import StrEnum


class GetDocumentArtifactManifestResponse413Code(StrEnum):
    INSPECTION_LIMIT_EXCEEDED = "inspection_limit_exceeded"

    def __str__(self) -> str:
        return str(self.value)
