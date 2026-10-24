from enum import StrEnum


class MemoryEntityProposalOptionsAbsorbed(StrEnum):
    REJECT = "reject"
    RELEASE = "release"
    SUPPRESS = "suppress"

    def __str__(self) -> str:
        return str(self.value)
