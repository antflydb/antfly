from enum import StrEnum


class ResolverResolvedEntityDecision(StrEnum):
    MATCH = "match"
    NEW = "new"
    REVIEW = "review"
    SUPPRESSED = "suppressed"

    def __str__(self) -> str:
        return str(self.value)
