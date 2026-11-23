from enum import StrEnum


class DocumentSQLColumnType(StrEnum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    JSON = "json"
    KEYWORD = "keyword"
    NUMBER = "number"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
