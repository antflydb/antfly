from enum import Enum


class GraphTraversalMetricFreshness(str, Enum):
    FRESH = "fresh"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
