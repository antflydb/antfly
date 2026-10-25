from enum import StrEnum


class ChunkOutputResultStatus(StrEnum):
    COMPLETE = "complete"
    FAILED = "failed"
    SKIPPED = "skipped"

    def __str__(self) -> str:
        return str(self.value)
