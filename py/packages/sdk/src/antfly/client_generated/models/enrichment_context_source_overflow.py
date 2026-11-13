from enum import StrEnum


class EnrichmentContextSourceOverflow(StrEnum):
    ERROR = "error"

    def __str__(self) -> str:
        return str(self.value)
