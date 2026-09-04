from enum import StrEnum


class ListDocumentArtifactManifestsDetail(StrEnum):
    RAW = "raw"
    SUMMARY = "summary"

    def __str__(self) -> str:
        return str(self.value)
