from enum import StrEnum


class MemoryRelationshipProposalAssertionStability(StrEnum):
    PROVISIONAL = "provisional"
    SETTLED = "settled"

    def __str__(self) -> str:
        return str(self.value)
