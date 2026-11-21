from enum import StrEnum


class EnrichmentContextSourceTracking(StrEnum):
    LIVE = "live"
    SNAPSHOT = "snapshot"

    def __str__(self) -> str:
        return str(self.value)
