from enum import Enum


class TransactionCommitResponseStatus(str, Enum):
    ABORTED = "aborted"
    COMMITTED = "committed"
    COMMITTED_RECOVERY_PENDING = "committed_recovery_pending"
    COMMITTED_VISIBILITY_PENDING = "committed_visibility_pending"

    def __str__(self) -> str:
        return str(self.value)
