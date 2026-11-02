from enum import StrEnum


class Style(StrEnum):
    JSON = "json"
    LOGFMT = "logfmt"
    NOOP = "noop"
    TERMINAL = "terminal"

    def __str__(self) -> str:
        return str(self.value)
