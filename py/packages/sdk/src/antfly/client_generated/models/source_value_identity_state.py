from enum import StrEnum


class SourceValueIdentityState(StrEnum):
    MISSING = "missing"
    UNCHANGED = "unchanged"
    VALUE = "value"

    def __str__(self) -> str:
        return str(self.value)
