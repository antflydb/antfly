from enum import Enum


class CalendarInterval(str, Enum):
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    MONTH = "month"
    QUARTER = "quarter"
    WEEK = "week"
    YEAR = "year"

    def __str__(self) -> str:
        return str(self.value)
