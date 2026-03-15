from enum import Enum


class BingSearchConfigFreshness(str, Enum):
    DAY = "Day"
    MONTH = "Month"
    WEEK = "Week"

    def __str__(self) -> str:
        return str(self.value)
