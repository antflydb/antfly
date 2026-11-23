from enum import StrEnum


class SQLCatalogObjectKind(StrEnum):
    AGGREGATE = "aggregate"
    AUTHORIZATION = "authorization"
    CAST = "cast"
    COLLATION = "collation"
    DATABASE = "database"
    EXTENSION = "extension"
    FUNCTION = "function"
    INDEX = "index"
    NAMESPACE = "namespace"
    OPERATOR = "operator"
    PROCEDURE = "procedure"
    QUERY_SOURCE = "query_source"
    SEQUENCE = "sequence"
    PUBLICATION = "publication"
    SUBSCRIPTION = "subscription"
    TABLE = "table"
    TABLESPACE = "tablespace"
    TYPE = "type"
    VIEW = "view"

    def __str__(self) -> str:
        return str(self.value)
