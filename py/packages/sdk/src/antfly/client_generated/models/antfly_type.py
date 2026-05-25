from enum import Enum


class AntflyType(str, Enum):
    BLOB = "blob"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    EMBEDDING = "embedding"
    GEOPOINT = "geopoint"
    GEOSHAPE = "geoshape"
    HTML = "html"
    KEYWORD = "keyword"
    LINK = "link"
    NUMERIC = "numeric"
    SEARCH_AS_YOU_TYPE = "search_as_you_type"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
