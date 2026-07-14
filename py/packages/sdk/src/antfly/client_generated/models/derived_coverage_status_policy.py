from enum import Enum


class DerivedCoverageStatusPolicy(str, Enum):
    BEST_EFFORT = "best_effort"
    EXTERNAL = "external"
    PARTIAL = "partial"
    STRICT = "strict"

    def __str__(self) -> str:
        return str(self.value)
