from enum import IntEnum


class ExtractionUnitSeedPageRotation(IntEnum):
    VALUE_0 = 0
    VALUE_90 = 90
    VALUE_180 = 180
    VALUE_270 = 270

    def __str__(self) -> str:
        return str(self.value)
