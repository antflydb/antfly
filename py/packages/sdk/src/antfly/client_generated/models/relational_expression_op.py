from enum import StrEnum


class RelationalExpressionOp(StrEnum):
    ADD = "add"
    AND = "and"
    CASE_WHEN = "case_when"
    CAST = "cast"
    COALESCE = "coalesce"
    COLUMN = "column"
    CONCAT = "concat"
    DIVIDE = "divide"
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    IN_LIST = "in_list"
    IS_DISTINCT = "is_distinct"
    IS_NOT_DISTINCT = "is_not_distinct"
    IS_NOT_NULL = "is_not_null"
    IS_NULL = "is_null"
    LITERAL = "literal"
    LOWER_ASCII = "lower_ascii"
    LT = "lt"
    LTE = "lte"
    MODULO = "modulo"
    MULTIPLY = "multiply"
    NE = "ne"
    NEGATE = "negate"
    NOT = "not"
    NOT_IN_LIST = "not_in_list"
    OR = "or"
    SUBTRACT = "subtract"
    UPPER_ASCII = "upper_ascii"

    def __str__(self) -> str:
        return str(self.value)
