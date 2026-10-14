from enum import StrEnum


class GraphWriteReceiptStatus(StrEnum):
    ABORTED = "aborted"
    COMMITTED = "committed"
    COMMITTED_PENDING = "committed_pending"
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
