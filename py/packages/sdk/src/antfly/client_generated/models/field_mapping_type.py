from enum import Enum


class FieldMappingType(str, Enum):
    BLOB = "blob"
    BOOL = "bool"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    EMBEDDING = "embedding"
    GEOPOINT = "geopoint"
    GEOSHAPE = "geoshape"
    GEO_POINT = "geo_point"
    GEO_SHAPE = "geo_shape"
    HTML = "html"
    INTEGER = "integer"
    KEYWORD = "keyword"
    LINK = "link"
    NUMBER = "number"
    NUMERIC = "numeric"
    SEARCH_AS_YOU_TYPE = "search_as_you_type"
    TEXT = "text"
    TIMESTAMP = "timestamp"

    def __str__(self) -> str:
        return str(self.value)
