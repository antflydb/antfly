from enum import StrEnum


class AlgebraicAggregationJoinKind(StrEnum):
    BUCKET = "bucket"
    BUCKET_WINDOW = "bucket_window"
    NONE = "none"
    WINDOW = "window"

    def __str__(self) -> str:
        return str(self.value)
