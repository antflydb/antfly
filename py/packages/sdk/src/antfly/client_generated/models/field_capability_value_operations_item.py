from enum import StrEnum


class FieldCapabilityValueOperationsItem(StrEnum):
    AUTOCOMPLETE = "autocomplete"
    CONTAINS = "contains"
    EQUALS = "equals"
    MATCHES = "matches"

    def __str__(self) -> str:
        return str(self.value)
