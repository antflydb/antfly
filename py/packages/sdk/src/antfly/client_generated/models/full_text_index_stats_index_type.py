from enum import StrEnum


class FullTextIndexStatsIndexType(StrEnum):
    FULL_TEXT = "full_text"

    def __str__(self) -> str:
        return str(self.value)
