from enum import StrEnum


class CurationProposalDecisionResultAtomicity(StrEnum):
    NAMESPACE = "namespace"
    RESOLVER = "resolver"

    def __str__(self) -> str:
        return str(self.value)
