from enum import Enum


class FieldCapabilityQueryModesItem(str, Enum):
    AUTOCOMPLETE = "autocomplete"
    EXACT = "exact"
    FULL_TEXT = "full_text"
    GEO = "geo"
    RANGE = "range"

    def __str__(self) -> str:
        return str(self.value)
