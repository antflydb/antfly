from enum import Enum


class SerperSearchConfigTimePeriod(str, Enum):
    D = "d"
    M = "m"
    W = "w"
    Y = "y"

    def __str__(self) -> str:
        return str(self.value)
