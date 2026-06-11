from enum import Enum


class DocumentArtifactTableReprocessResponseReprocess(str, Enum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
