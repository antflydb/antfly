from enum import StrEnum


class AlgebraicAggregateConfigOp(StrEnum):
    AVG = "avg"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    SUM = "sum"

    def __str__(self) -> str:
        return str(self.value)
