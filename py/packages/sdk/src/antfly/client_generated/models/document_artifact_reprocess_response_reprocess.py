from enum import StrEnum


class DocumentArtifactReprocessResponseReprocess(StrEnum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
