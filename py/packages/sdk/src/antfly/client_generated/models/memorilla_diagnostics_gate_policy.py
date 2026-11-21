from enum import StrEnum


class MemorillaDiagnosticsGatePolicy(StrEnum):
    ALWAYS = "always"
    LEARNED = "learned"
    ROUTED = "routed"

    def __str__(self) -> str:
        return str(self.value)
