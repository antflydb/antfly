from enum import StrEnum


class QueryRetainedDatasetResponse200MeasuresItemLaw(StrEnum):
    AVG = "avg"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    SUM = "sum"

    def __str__(self) -> str:
        return str(self.value)
