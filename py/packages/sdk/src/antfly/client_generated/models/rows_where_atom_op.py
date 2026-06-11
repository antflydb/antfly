from enum import Enum


class RowsWhereAtomOp(str, Enum):
    ARRAY_ANY = "array_any"
    ARRAY_CONTAINS = "array_contains"
    ARRAY_EQ = "array_eq"
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    IN = "in"
    IS_DISTINCT = "is_distinct"
    IS_NOT_DISTINCT = "is_not_distinct"
    IS_NOT_NULL = "is_not_null"
    IS_NULL = "is_null"
    JSON_CONTAINS = "json_contains"
    JSON_PATH_EQ = "json_path_eq"
    JSON_PATH_EXISTS = "json_path_exists"
    LT = "lt"
    LTE = "lte"
    NE = "ne"
    NOT_IN = "not_in"
    TEXT_PATTERN = "text_pattern"

    def __str__(self) -> str:
        return str(self.value)
