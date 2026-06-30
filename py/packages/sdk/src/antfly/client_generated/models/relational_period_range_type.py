from enum import Enum


class RelationalPeriodRangeType(str, Enum):
    DATERANGE = "daterange"
    NUMRANGE = "numrange"
    TSRANGE = "tsrange"
    TSTZRANGE = "tstzrange"

    def __str__(self) -> str:
        return str(self.value)
