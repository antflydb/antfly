from enum import StrEnum


class MemoryEntityCorrectionAction(StrEnum):
    MERGE = "merge"
    SUPPRESS = "suppress"
    UNMERGE = "unmerge"
    UNSUPPRESS = "unsuppress"

    def __str__(self) -> str:
        return str(self.value)
