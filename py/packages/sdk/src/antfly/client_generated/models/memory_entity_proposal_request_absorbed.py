from enum import StrEnum


class MemoryEntityProposalRequestAbsorbed(StrEnum):
    REJECT = "reject"
    RELEASE = "release"
    SUPPRESS = "suppress"

    def __str__(self) -> str:
        return str(self.value)
