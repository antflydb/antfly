from enum import StrEnum


class ProvenanceDecisionInitiatorAssurance(StrEnum):
    AUTHENTICATED = "authenticated"
    DECLARED = "declared"

    def __str__(self) -> str:
        return str(self.value)
