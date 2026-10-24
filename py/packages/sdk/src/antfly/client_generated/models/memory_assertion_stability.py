from enum import StrEnum


class MemoryAssertionStability(StrEnum):
    PROVISIONAL = "provisional"
    SETTLED = "settled"

    def __str__(self) -> str:
        return str(self.value)
