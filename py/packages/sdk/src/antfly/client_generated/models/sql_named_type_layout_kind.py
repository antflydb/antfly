from enum import StrEnum


class SQLNamedTypeLayoutKind(StrEnum):
    ENUM = "enum"

    def __str__(self) -> str:
        return str(self.value)
