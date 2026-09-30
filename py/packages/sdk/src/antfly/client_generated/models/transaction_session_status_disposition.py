from enum import StrEnum


class TransactionSessionStatusDisposition(StrEnum):
    ABORTED = "aborted"
    ACTIVE = "active"
    COMMITTED = "committed"
    COMMITTED_PENDING = "committed_pending"
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"
    OUTCOME_UNKNOWN = "outcome_unknown"

    def __str__(self) -> str:
        return str(self.value)
