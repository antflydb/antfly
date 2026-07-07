from enum import Enum


class FieldCapabilitySortLifecycleState(str, Enum):
    ACCELERATED = "accelerated"
    COVERED = "covered"
    DECLARED = "declared"
    INDEXED = "indexed"
    QUERYABLE = "queryable"
    UNSUPPORTED = "unsupported"

    def __str__(self) -> str:
        return str(self.value)
