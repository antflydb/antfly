from enum import StrEnum


class MemoryRecordLifecycle(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    RETRACTED = "retracted"
    SUPERSEDED = "superseded"

    def __str__(self) -> str:
        return str(self.value)
