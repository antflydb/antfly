from enum import Enum


class DistanceUnit(str, Enum):
    FT = "ft"
    KM = "km"
    M = "m"
    MI = "mi"
    YD = "yd"

    def __str__(self) -> str:
        return str(self.value)
