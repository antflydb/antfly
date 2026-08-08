from enum import Enum


class MultiBatchResponseStatus(str, Enum):
    COMMITTED = "committed"
    COMMITTED_RECOVERY_PENDING = "committed_recovery_pending"
    COMMITTED_VISIBILITY_PENDING = "committed_visibility_pending"

    def __str__(self) -> str:
        return str(self.value)
