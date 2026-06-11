from enum import Enum


class DocumentArtifactReprocessJobReprocessStatus(str, Enum):
    COMPLETE = "complete"
    IN_PROGRESS = "in_progress"
    STOPPED = "stopped"

    def __str__(self) -> str:
        return str(self.value)
