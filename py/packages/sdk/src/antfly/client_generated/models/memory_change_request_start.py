from enum import StrEnum


class MemoryChangeRequestStart(StrEnum):
    BEGINNING = "beginning"
    LATEST = "latest"

    def __str__(self) -> str:
        return str(self.value)
