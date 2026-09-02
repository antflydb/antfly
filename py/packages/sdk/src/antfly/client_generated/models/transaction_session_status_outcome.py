from enum import Enum


class TransactionSessionStatusOutcome(str, Enum):
    ABORTED = "aborted"
    COMMITTED = "committed"
    COMMITTED_RECOVERY_PENDING = "committed_recovery_pending"
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"
    COMMITTED_VISIBILITY_PENDING = "committed_visibility_pending"
    NOT_APPLIED = "not_applied"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
