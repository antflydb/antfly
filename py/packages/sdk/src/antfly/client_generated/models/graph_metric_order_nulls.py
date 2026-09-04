from enum import StrEnum


class GraphMetricOrderNulls(StrEnum):
    FIRST = "first"
    LAST = "last"
    NULLS_FIRST = "nulls_first"
    NULLS_LAST = "nulls_last"

    def __str__(self) -> str:
        return str(self.value)
