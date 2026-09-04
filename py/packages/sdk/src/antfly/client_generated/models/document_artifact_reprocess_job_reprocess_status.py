from enum import StrEnum


class DocumentArtifactReprocessJobReprocessStatus(StrEnum):
    COMPLETE = "complete"
    IN_PROGRESS = "in_progress"
    STOPPED = "stopped"

    def __str__(self) -> str:
        return str(self.value)
