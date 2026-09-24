from enum import StrEnum


class GraphMetricQueryMetricFreshness(StrEnum):
    FRESH = "fresh"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
