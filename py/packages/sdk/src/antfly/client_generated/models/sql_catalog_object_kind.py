from enum import StrEnum


class SQLCatalogObjectKind(StrEnum):
    FUNCTION = "function"
    PROCEDURE = "procedure"
    SEQUENCE = "sequence"

    def __str__(self) -> str:
        return str(self.value)
