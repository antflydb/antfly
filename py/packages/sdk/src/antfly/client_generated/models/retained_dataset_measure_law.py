from enum import StrEnum


class RetainedDatasetMeasureLaw(StrEnum):
    AVG = "avg"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    SUM = "sum"

    def __str__(self) -> str:
        return str(self.value)
