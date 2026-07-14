from enum import Enum


class DerivedCoveragePolicy(str, Enum):
    BEST_EFFORT = "best_effort"
    PARTIAL = "partial"
    STRICT = "strict"

    def __str__(self) -> str:
        return str(self.value)
