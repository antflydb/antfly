from enum import StrEnum


class IndexReadinessState(StrEnum):
    FAILED = "failed"
    PENDING = "pending"
    QUERYABLE_PARTIAL = "queryable_partial"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
