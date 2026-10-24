from enum import StrEnum


class MemoryRelationshipProposalRequestAssertionRole(StrEnum):
    HYPOTHESIZED = "hypothesized"
    INFERRED = "inferred"
    OBSERVED = "observed"
    REPORTED = "reported"

    def __str__(self) -> str:
        return str(self.value)
