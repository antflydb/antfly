from enum import StrEnum


class RetainedDatasetOwnerProgressMode(StrEnum):
    CLOSED = "closed"
    INSTALLED = "installed"
    READY = "ready"
    REGISTERED = "registered"
    REVOKED = "revoked"

    def __str__(self) -> str:
        return str(self.value)
