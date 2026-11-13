from enum import StrEnum


class EnrichmentRefreshConfigMode(StrEnum):
    BACKGROUND = "background"
    MANUAL = "manual"

    def __str__(self) -> str:
        return str(self.value)
