from enum import StrEnum


class InferenceContextPrepareRequestMode(StrEnum):
    COMPACT = "compact"
    INSPECT = "inspect"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
