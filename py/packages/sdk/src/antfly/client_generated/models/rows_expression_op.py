from enum import Enum


class RowsExpressionOp(str, Enum):
    ADD = "add"
    ARRAY_LENGTH = "array_length"
    CASE = "case"
    CAST = "cast"
    COALESCE = "coalesce"
    CONCAT = "concat"
    DIV = "div"
    JSON_EXTRACT = "json_extract"
    LOWER = "lower"
    MUL = "mul"
    NOW = "now"
    NULLIF = "nullif"
    STRING_TO_ARRAY = "string_to_array"
    SUB = "sub"
    UPPER = "upper"

    def __str__(self) -> str:
        return str(self.value)
