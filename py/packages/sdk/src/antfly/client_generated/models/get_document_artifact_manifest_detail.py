from enum import StrEnum


class GetDocumentArtifactManifestDetail(StrEnum):
    RAW = "raw"
    SUMMARY = "summary"

    def __str__(self) -> str:
        return str(self.value)
