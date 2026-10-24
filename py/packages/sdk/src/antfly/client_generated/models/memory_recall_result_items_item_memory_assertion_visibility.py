from enum import StrEnum


class MemoryRecallResultItemsItemMemoryAssertionVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
