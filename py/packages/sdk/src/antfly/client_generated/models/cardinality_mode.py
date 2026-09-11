from enum import StrEnum


class CardinalityMode(StrEnum):
    APPROXIMATE = "approximate"
    AUTO = "auto"
    EXACT = "exact"

    def __str__(self) -> str:
        return str(self.value)
