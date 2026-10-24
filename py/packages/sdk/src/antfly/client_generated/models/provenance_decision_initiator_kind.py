from enum import StrEnum


class ProvenanceDecisionInitiatorKind(StrEnum):
    AGENT = "agent"
    SERVICE = "service"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
