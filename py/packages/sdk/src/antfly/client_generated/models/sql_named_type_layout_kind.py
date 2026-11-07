from enum import StrEnum


class SQLNamedTypeLayoutKind(StrEnum):
    DOMAIN = "domain"
    ENUM = "enum"

    def __str__(self) -> str:
        return str(self.value)
