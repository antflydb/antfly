from enum import Enum


class DocumentArtifactReprocessResponseReprocess(str, Enum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
