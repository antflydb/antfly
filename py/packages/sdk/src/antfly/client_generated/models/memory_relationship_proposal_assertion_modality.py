from enum import StrEnum


class MemoryRelationshipProposalAssertionModality(StrEnum):
    ACTUAL = "actual"
    CONDITIONAL = "conditional"
    DESIRED = "desired"
    HYPOTHETICAL = "hypothetical"
    PLANNED = "planned"

    def __str__(self) -> str:
        return str(self.value)
