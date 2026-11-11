from enum import StrEnum


class RetainedSubscriptionFrameKind(StrEnum):
    CHANGES = "changes"
    SNAPSHOT = "snapshot"

    def __str__(self) -> str:
        return str(self.value)
