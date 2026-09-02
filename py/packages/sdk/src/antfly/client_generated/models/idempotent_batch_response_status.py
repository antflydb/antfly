from enum import Enum


class IdempotentBatchResponseStatus(str, Enum):
    COMMITTED = "committed"
    COMMITTED_PENDING = "committed_pending"
    COMMITTED_RECOVERY_PENDING = "committed_recovery_pending"
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"
    COMMITTED_VISIBILITY_PENDING = "committed_visibility_pending"

    def __str__(self) -> str:
        return str(self.value)
