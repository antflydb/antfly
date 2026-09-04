from enum import StrEnum


class FilterSpecOperator(StrEnum):
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
