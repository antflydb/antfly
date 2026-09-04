from enum import StrEnum


class DocumentArtifactTableReprocessResponseReprocessStatus(StrEnum):
    COMPLETE = "complete"
    IN_PROGRESS = "in_progress"

    def __str__(self) -> str:
        return str(self.value)
