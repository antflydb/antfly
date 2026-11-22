from enum import StrEnum


class DocumentArtifactTableReprocessRequestSelection(StrEnum):
    ALL = "all"
    INCOMPLETE = "incomplete"

    def __str__(self) -> str:
        return str(self.value)
