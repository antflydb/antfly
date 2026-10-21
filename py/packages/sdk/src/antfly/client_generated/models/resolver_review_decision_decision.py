from enum import StrEnum


class ResolverReviewDecisionDecision(StrEnum):
    MATCH = "match"
    NEW = "new"
    SUPPRESSED = "suppressed"

    def __str__(self) -> str:
        return str(self.value)
