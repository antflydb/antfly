from enum import StrEnum


class MemoryEntityProposalState(StrEnum):
    ACCEPTED = "accepted"
    OPEN = "open"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"

    def __str__(self) -> str:
        return str(self.value)
