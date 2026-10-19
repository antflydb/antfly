from enum import StrEnum


class FieldCapabilityQueryModesItem(StrEnum):
    AUTOCOMPLETE = "autocomplete"
    EXACT = "exact"
    FULL_TEXT = "full_text"
    GEO = "geo"
    RANGE = "range"
    SUBSTRING = "substring"

    def __str__(self) -> str:
        return str(self.value)
