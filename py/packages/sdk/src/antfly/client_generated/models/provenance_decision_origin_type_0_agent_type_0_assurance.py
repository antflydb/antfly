from enum import StrEnum


class ProvenanceDecisionOriginType0AgentType0Assurance(StrEnum):
    AUTHENTICATED = "authenticated"
    DECLARED = "declared"

    def __str__(self) -> str:
        return str(self.value)
