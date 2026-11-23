from enum import StrEnum


class DocumentArtifactReprocessJobStartRequestSelection(StrEnum):
    ALL = "all"
    INCOMPLETE = "incomplete"

    def __str__(self) -> str:
        return str(self.value)
