from enum import Enum


class LegacyGraphQueryMetricFreshness(str, Enum):
    FRESH = "fresh"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
