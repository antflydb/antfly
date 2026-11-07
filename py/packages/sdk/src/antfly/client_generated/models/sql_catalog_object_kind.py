from enum import StrEnum


class SQLCatalogObjectKind(StrEnum):
    AGGREGATE = "aggregate"
    AUTHORIZATION = "authorization"
    CAST = "cast"
    DATABASE = "database"
    FUNCTION = "function"
    INDEX = "index"
    NAMESPACE = "namespace"
    OPERATOR = "operator"
    PROCEDURE = "procedure"
    QUERY_SOURCE = "query_source"
    SEQUENCE = "sequence"
    TABLE = "table"
    TABLESPACE = "tablespace"
    TYPE = "type"
    VIEW = "view"

    def __str__(self) -> str:
        return str(self.value)
