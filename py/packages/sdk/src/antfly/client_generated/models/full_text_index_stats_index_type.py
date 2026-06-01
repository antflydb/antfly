from enum import Enum


class FullTextIndexStatsIndexType(str, Enum):
    FULL_TEXT = "full_text"

    def __str__(self) -> str:
        return str(self.value)
