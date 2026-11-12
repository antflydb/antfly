from enum import StrEnum


class RetainedDatasetInfoPhase(StrEnum):
    ACTIVATING = "activating"
    ACTIVE = "active"
    CLOSED = "closed"
    CLOSING = "closing"
    RETIRING = "retiring"

    def __str__(self) -> str:
        return str(self.value)
