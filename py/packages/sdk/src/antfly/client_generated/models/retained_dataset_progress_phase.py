from enum import StrEnum


class RetainedDatasetProgressPhase(StrEnum):
    ACTIVATING = "activating"
    ACTIVE = "active"
    CLOSED = "closed"
    CLOSING = "closing"
    RETIRED = "retired"
    RETIRING = "retiring"

    def __str__(self) -> str:
        return str(self.value)
