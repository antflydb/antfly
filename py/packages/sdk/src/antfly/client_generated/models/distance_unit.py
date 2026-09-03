from enum import StrEnum


class DistanceUnit(StrEnum):
    FT = "ft"
    KM = "km"
    M = "m"
    MI = "mi"
    YD = "yd"

    def __str__(self) -> str:
        return str(self.value)
