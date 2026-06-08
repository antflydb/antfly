from enum import Enum


class RowsUniquePredicateOp(str, Enum):
    EQ = "eq"
    IS_NOT_NULL = "is_not_null"
    IS_NULL = "is_null"
    NE = "ne"

    def __str__(self) -> str:
        return str(self.value)
