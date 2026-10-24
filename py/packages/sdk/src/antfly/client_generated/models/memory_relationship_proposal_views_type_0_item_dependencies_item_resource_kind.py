from enum import StrEnum


class MemoryRelationshipProposalViewsType0ItemDependenciesItemResourceKind(StrEnum):
    ASSERTION = "assertion"
    CONFIGURATION = "configuration"
    DECISION = "decision"
    ENTITY = "entity"
    EVIDENCE = "evidence"
    OPERATION = "operation"
    OUTCOME = "outcome"
    RECORD = "record"
    RELATIONSHIP = "relationship"
    TABLE = "table"
    TRACE = "trace"

    def __str__(self) -> str:
        return str(self.value)
