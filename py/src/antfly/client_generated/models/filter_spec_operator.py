from enum import Enum


class FilterSpecOperator(str, Enum):
    CONTAINS = "contains"
    EQ = "eq"
    GT = "gt"
    GTE = "gte"
    IN = "in"
    LT = "lt"
    LTE = "lte"
    NE = "ne"
    PREFIX = "prefix"
    RANGE = "range"

    def __str__(self) -> str:
        return str(self.value)
