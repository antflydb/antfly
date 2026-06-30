from enum import Enum


class RowsAggregateSpecOp(str, Enum):
    ARRAY_AGG = "array_agg"
    AVG = "avg"
    BOOL_AND = "bool_and"
    BOOL_OR = "bool_or"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    MODE = "mode"
    PERCENTILE_CONT = "percentile_cont"
    PERCENTILE_DISC = "percentile_disc"
    STRING_AGG = "string_agg"
    SUM = "sum"

    def __str__(self) -> str:
        return str(self.value)
