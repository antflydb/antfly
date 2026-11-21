from enum import StrEnum


class MemorillaDiagnosticsReadMode(StrEnum):
    ALL = "all"
    AUTO = "auto"
    NARROW = "narrow"
    NONE = "none"
    WIDE = "wide"

    def __str__(self) -> str:
        return str(self.value)
