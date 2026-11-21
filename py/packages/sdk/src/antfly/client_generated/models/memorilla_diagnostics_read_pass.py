from enum import StrEnum


class MemorillaDiagnosticsReadPass(StrEnum):
    NARROW = "narrow"
    WIDE = "wide"

    def __str__(self) -> str:
        return str(self.value)
