from enum import StrEnum


class SQLNamedDomainBaseKind(StrEnum):
    BUILTIN = "builtin"
    NAMED = "named"

    def __str__(self) -> str:
        return str(self.value)
