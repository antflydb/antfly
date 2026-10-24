from enum import StrEnum


class MemoryRecordPageItemsItemAssertionVisibility(StrEnum):
    PRIVATE = "private"
    TEAM = "team"

    def __str__(self) -> str:
        return str(self.value)
