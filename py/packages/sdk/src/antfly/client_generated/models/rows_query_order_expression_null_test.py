from enum import Enum


class RowsQueryOrderExpressionNullTest(str, Enum):
    IS_NOT_NULL = "is_not_null"
    IS_NULL = "is_null"

    def __str__(self) -> str:
        return str(self.value)
