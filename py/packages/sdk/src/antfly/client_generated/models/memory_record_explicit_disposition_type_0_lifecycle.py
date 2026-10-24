from enum import StrEnum


class MemoryRecordExplicitDispositionType0Lifecycle(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    RETRACTED = "retracted"
    SUPERSEDED = "superseded"

    def __str__(self) -> str:
        return str(self.value)
