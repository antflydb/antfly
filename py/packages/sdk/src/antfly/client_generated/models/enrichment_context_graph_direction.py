from enum import StrEnum


class EnrichmentContextGraphDirection(StrEnum):
    BOTH = "both"
    IN = "in"
    OUT = "out"

    def __str__(self) -> str:
        return str(self.value)
