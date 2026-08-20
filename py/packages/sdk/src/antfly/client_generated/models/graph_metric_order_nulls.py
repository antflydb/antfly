from enum import Enum


class GraphMetricOrderNulls(str, Enum):
    FIRST = "first"
    LAST = "last"
    NULLS_FIRST = "nulls_first"
    NULLS_LAST = "nulls_last"

    def __str__(self) -> str:
        return str(self.value)
