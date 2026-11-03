from enum import StrEnum


class QuantizationRefine(StrEnum):
    ALL = "all"
    NEAR_CUTOFF = "near_cutoff"

    def __str__(self) -> str:
        return str(self.value)
