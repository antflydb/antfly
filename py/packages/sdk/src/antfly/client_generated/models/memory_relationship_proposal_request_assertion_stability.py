from enum import StrEnum


class MemoryRelationshipProposalRequestAssertionStability(StrEnum):
    PROVISIONAL = "provisional"
    SETTLED = "settled"

    def __str__(self) -> str:
        return str(self.value)
