from enum import Enum


class RowsQueryRequestTotalMode(str, Enum):
    BOUNDED = "bounded"
    EXACT = "exact"
    NONE = "none"

    def __str__(self) -> str:
        return str(self.value)
