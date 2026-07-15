from enum import Enum


class AlgebraicIndexStatsPlannerLastUnsupportedReason(str, Enum):
    ACCESS_METHOD_CAPABILITY_MISMATCH = "access-method-capability-mismatch"
    INDEX_NOT_READY = "index-not-ready"
    ORDERING_NOT_COVERED = "ordering-not-covered"
    PREDICATE_NOT_PROVEN = "predicate-not-proven"
    STALE_GENERATION = "stale-generation"
    UNSUPPORTED_ACCESS_METHOD = "unsupported-access-method"

    def __str__(self) -> str:
        return str(self.value)
