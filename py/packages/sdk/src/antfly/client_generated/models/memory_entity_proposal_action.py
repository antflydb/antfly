from enum import StrEnum


class MemoryEntityProposalAction(StrEnum):
    DISTINCT = "distinct"
    MERGE = "merge"
    SUPPRESS = "suppress"
    UNMERGE = "unmerge"
    UNSUPPRESS = "unsuppress"

    def __str__(self) -> str:
        return str(self.value)
