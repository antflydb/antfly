from enum import StrEnum


class SQLCatalogObjectKind(StrEnum):
    AGGREGATE = "aggregate"
    CAST = "cast"
    FUNCTION = "function"
    OPERATOR = "operator"
    PROCEDURE = "procedure"
    SEQUENCE = "sequence"
    TYPE = "type"

    def __str__(self) -> str:
        return str(self.value)
