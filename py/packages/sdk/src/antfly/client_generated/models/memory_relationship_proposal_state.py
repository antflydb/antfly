from enum import StrEnum


class MemoryRelationshipProposalState(StrEnum):
    ACCEPTED = "accepted"
    OPEN = "open"
    REJECTED = "rejected"

    def __str__(self) -> str:
        return str(self.value)
