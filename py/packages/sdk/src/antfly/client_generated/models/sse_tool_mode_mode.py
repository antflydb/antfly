from enum import StrEnum


class SSEToolModeMode(StrEnum):
    NATIVE = "native"
    STRUCTURED_OUTPUT = "structured_output"

    def __str__(self) -> str:
        return str(self.value)
