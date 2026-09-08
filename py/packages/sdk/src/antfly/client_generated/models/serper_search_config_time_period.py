from enum import StrEnum


class SerperSearchConfigTimePeriod(StrEnum):
    D = "d"
    M = "m"
    W = "w"
    Y = "y"

    def __str__(self) -> str:
        return str(self.value)
