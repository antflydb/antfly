from enum import StrEnum


class DerivedCoverageStatusPolicy(StrEnum):
    BEST_EFFORT = "best_effort"
    EXTERNAL = "external"
    PARTIAL = "partial"
    STRICT = "strict"

    def __str__(self) -> str:
        return str(self.value)
