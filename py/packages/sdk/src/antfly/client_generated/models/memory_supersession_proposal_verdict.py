from enum import StrEnum


class MemorySupersessionProposalVerdict(StrEnum):
    CONTRADICTS = "contradicts"
    INDEPENDENT = "independent"
    RESTATES = "restates"
    SUPERSEDES = "supersedes"

    def __str__(self) -> str:
        return str(self.value)
