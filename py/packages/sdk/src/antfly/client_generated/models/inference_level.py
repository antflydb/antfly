from enum import StrEnum


class InferenceLevel(StrEnum):
    DEBUG = "debug"
    ERROR = "error"
    INFO = "info"
    WARN = "warn"

    def __str__(self) -> str:
        return str(self.value)
