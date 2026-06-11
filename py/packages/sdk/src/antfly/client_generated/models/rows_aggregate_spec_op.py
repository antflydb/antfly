from enum import Enum


class RowsAggregateSpecOp(str, Enum):
    ARRAY_AGG = "array_agg"
    AVG = "avg"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    SUM = "sum"

    def __str__(self) -> str:
        return str(self.value)
