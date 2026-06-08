from enum import Enum


class RowsExpressionConditionOp(str, Enum):
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    IS_DISTINCT = "is_distinct"
    IS_NOT_DISTINCT = "is_not_distinct"
    IS_NOT_NULL = "is_not_null"
    IS_NULL = "is_null"
    LT = "lt"
    LTE = "lte"
    NE = "ne"

    def __str__(self) -> str:
        return str(self.value)
