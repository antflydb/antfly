from enum import StrEnum


class InferenceContextDiagnosticsPolicy(StrEnum):
    COMPACT = "compact"
    INSPECT = "inspect"
    REJECT = "reject"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
