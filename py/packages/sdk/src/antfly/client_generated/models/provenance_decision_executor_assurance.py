from enum import StrEnum


class ProvenanceDecisionExecutorAssurance(StrEnum):
    AUTHENTICATED = "authenticated"
    DECLARED = "declared"

    def __str__(self) -> str:
        return str(self.value)
