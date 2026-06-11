from enum import Enum


class ListDocumentArtifactManifestsDetail(str, Enum):
    RAW = "raw"
    SUMMARY = "summary"

    def __str__(self) -> str:
        return str(self.value)
