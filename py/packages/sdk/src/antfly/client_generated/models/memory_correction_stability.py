from enum import StrEnum


class MemoryCorrectionStability(StrEnum):
    PROVISIONAL = "provisional"
    SETTLED = "settled"

    def __str__(self) -> str:
        return str(self.value)
