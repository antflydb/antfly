from enum import Enum


class CardinalityMode(str, Enum):
    APPROXIMATE = "approximate"
    AUTO = "auto"
    EXACT = "exact"

    def __str__(self) -> str:
        return str(self.value)
