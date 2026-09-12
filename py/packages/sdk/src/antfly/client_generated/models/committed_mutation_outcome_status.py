from enum import StrEnum


class CommittedMutationOutcomeStatus(StrEnum):
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"
    COMMITTED_REPAIR_UNAVAILABLE = "committed_repair_unavailable"
    COMMITTED_SUPERSEDED = "committed_superseded"
    COMMITTED_VISIBILITY_PENDING = "committed_visibility_pending"

    def __str__(self) -> str:
        return str(self.value)
