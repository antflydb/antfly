from enum import StrEnum


class MemoryRecallResultItemsItemMemoryLifecycle(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    RETRACTED = "retracted"
    SUPERSEDED = "superseded"

    def __str__(self) -> str:
        return str(self.value)
