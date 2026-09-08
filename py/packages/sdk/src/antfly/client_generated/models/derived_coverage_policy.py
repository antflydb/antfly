from enum import StrEnum


class DerivedCoveragePolicy(StrEnum):
    BEST_EFFORT = "best_effort"
    PARTIAL = "partial"
    STRICT = "strict"

    def __str__(self) -> str:
        return str(self.value)
