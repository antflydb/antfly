from enum import StrEnum


class ForeignKeyTiming(StrEnum):
    DEFERRED = "deferred"
    IMMEDIATE = "immediate"

    def __str__(self) -> str:
        return str(self.value)
