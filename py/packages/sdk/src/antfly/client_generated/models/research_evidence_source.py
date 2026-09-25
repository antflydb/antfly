from enum import StrEnum


class ResearchEvidenceSource(StrEnum):
    FETCH = "fetch"
    TABLE = "table"
    WEB = "web"

    def __str__(self) -> str:
        return str(self.value)
