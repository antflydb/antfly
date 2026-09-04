from enum import StrEnum


class GraphMetricRerankMetricFreshness(StrEnum):
    FRESH = "fresh"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
