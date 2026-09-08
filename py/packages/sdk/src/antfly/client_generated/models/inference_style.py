from enum import StrEnum


class InferenceStyle(StrEnum):
    JSON = "json"
    LOGFMT = "logfmt"
    NOOP = "noop"
    TERMINAL = "terminal"

    def __str__(self) -> str:
        return str(self.value)
