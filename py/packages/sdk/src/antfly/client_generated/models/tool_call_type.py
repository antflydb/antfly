from enum import StrEnum


class ToolCallType(StrEnum):
    FUNCTION = "function"

    def __str__(self) -> str:
        return str(self.value)
