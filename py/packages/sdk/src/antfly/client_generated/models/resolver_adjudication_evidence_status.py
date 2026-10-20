from enum import StrEnum


class ResolverAdjudicationEvidenceStatus(StrEnum):
    ABSTAINED = "abstained"
    ACCEPTED = "accepted"
    BUDGET_EXHAUSTED = "budget_exhausted"
    FAILED = "failed"
    REJECTED = "rejected"

    def __str__(self) -> str:
        return str(self.value)
