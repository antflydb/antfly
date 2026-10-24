from enum import StrEnum


class MemoryRecordAssertionRole(StrEnum):
    HYPOTHESIZED = "hypothesized"
    INFERRED = "inferred"
    OBSERVED = "observed"
    REPORTED = "reported"

    def __str__(self) -> str:
        return str(self.value)
