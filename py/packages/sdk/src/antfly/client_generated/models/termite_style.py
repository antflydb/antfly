from enum import Enum


class TermiteStyle(str, Enum):
    JSON = "json"
    LOGFMT = "logfmt"
    NOOP = "noop"
    TERMINAL = "terminal"

    def __str__(self) -> str:
        return str(self.value)
