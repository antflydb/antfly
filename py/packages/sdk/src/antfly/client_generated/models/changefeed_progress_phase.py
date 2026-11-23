from enum import StrEnum


class ChangefeedProgressPhase(StrEnum):
    BOOTSTRAP = "bootstrap"
    CLOSING = "closing"
    DELETED = "deleted"
    ENROLLING = "enrolling"
    RESET_REQUIRED = "reset_required"
    STREAMING = "streaming"

    def __str__(self) -> str:
        return str(self.value)
