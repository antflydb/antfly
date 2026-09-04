from enum import StrEnum


class DocumentArtifactTableReprocessResponseReprocess(StrEnum):
    TRIGGERED = "triggered"

    def __str__(self) -> str:
        return str(self.value)
