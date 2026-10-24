from enum import StrEnum


class ProvenanceDecisionExecutorKind(StrEnum):
    AGENT = "agent"
    SERVICE = "service"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
