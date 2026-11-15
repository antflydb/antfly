from enum import StrEnum


class CommitRequires(StrEnum):
    DIRECT_ENRICHMENTS = "direct_enrichments"
    NONE = "none"

    def __str__(self) -> str:
        return str(self.value)
