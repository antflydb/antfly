from enum import StrEnum


class SQLCatalogObjectKind(StrEnum):
    AGGREGATE = "aggregate"
    AUTHORIZATION = "authorization"
    CAST = "cast"
    FUNCTION = "function"
    INDEX = "index"
    OPERATOR = "operator"
    PROCEDURE = "procedure"
    SEQUENCE = "sequence"
    TABLE = "table"
    TYPE = "type"

    def __str__(self) -> str:
        return str(self.value)
