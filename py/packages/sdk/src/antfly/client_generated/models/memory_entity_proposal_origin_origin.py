from enum import StrEnum


class MemoryEntityProposalOriginOrigin(StrEnum):
    ADJUDICATOR = "adjudicator"
    CASCADE = "cascade"
    CONFIG = "config"
    MANUAL = "manual"
    REVIEW = "review"

    def __str__(self) -> str:
        return str(self.value)
