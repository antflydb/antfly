from enum import StrEnum


class MemoryRecallResultItemsItemMemoryAssertionStability(StrEnum):
    PROVISIONAL = "provisional"
    SETTLED = "settled"

    def __str__(self) -> str:
        return str(self.value)
