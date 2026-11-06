from enum import StrEnum


class PrivacyScanPhase(StrEnum):
    FAILED = "failed"
    PROCESSING = "processing"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    SUPERSEDED = "superseded"

    def __str__(self) -> str:
        return str(self.value)
