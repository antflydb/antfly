from enum import StrEnum


class ProvenanceDecisionStatus(StrEnum):
    APPLIED = "applied"
    APPROVED = "approved"
    CANCELLED = "cancelled"
    PROPOSED = "proposed"
    REJECTED = "rejected"

    def __str__(self) -> str:
        return str(self.value)
