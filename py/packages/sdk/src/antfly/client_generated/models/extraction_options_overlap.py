from enum import StrEnum


class ExtractionOptionsOverlap(StrEnum):
    ALLOW = "allow"
    DISALLOW = "disallow"
    FLAT = "flat"
    LONGEST = "longest"
    NESTED = "nested"

    def __str__(self) -> str:
        return str(self.value)
