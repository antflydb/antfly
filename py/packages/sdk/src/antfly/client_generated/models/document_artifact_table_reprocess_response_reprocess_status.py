from enum import Enum


class DocumentArtifactTableReprocessResponseReprocessStatus(str, Enum):
    COMPLETE = "complete"
    IN_PROGRESS = "in_progress"

    def __str__(self) -> str:
        return str(self.value)
