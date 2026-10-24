from enum import StrEnum


class MemorySupersessionVerdictVerdict(StrEnum):
    CONTRADICTS = "contradicts"
    INDEPENDENT = "independent"
    RESTATES = "restates"
    SUPERSEDES = "supersedes"

    def __str__(self) -> str:
        return str(self.value)
