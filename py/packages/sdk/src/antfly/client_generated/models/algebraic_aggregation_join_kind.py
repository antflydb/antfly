from enum import Enum


class AlgebraicAggregationJoinKind(str, Enum):
    BUCKET = "bucket"
    BUCKET_WINDOW = "bucket_window"
    NONE = "none"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
