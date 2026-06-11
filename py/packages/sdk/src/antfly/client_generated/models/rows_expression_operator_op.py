from enum import Enum


class RowsExpressionOperatorOp(str, Enum):
    ABS = "abs"
    ADD = "add"
    ARRAY_LENGTH = "array_length"
    CASE = "case"
    CAST = "cast"
    CEIL = "ceil"
    COALESCE = "coalesce"
    CONCAT = "concat"
    DIV = "div"
    FLOOR = "floor"
    GREATEST = "greatest"
    INTERVAL_NS = "interval_ns"
    JSON_EXTRACT = "json_extract"
    LEAST = "least"
    LENGTH = "length"
    LOWER = "lower"
    MUL = "mul"
    NOW = "now"
    NULLIF = "nullif"
    REPLACE = "replace"
    ROUND = "round"
    STRING_TO_ARRAY = "string_to_array"
    SUB = "sub"
    TRIM = "trim"
    UPPER = "upper"

    def __str__(self) -> str:
        return str(self.value)
