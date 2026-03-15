from enum import Enum


class SSEToolModeMode(str, Enum):
    NATIVE = "native"
    STRUCTURED_OUTPUT = "structured_output"

    def __str__(self) -> str:
        return str(self.value)
