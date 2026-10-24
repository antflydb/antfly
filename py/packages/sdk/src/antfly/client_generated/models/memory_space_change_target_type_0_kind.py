from enum import StrEnum


class MemorySpaceChangeTargetType0Kind(StrEnum):
    ASSERTION = "assertion"
    CONFIGURATION = "configuration"
    ENTITY = "entity"
    ENTITY_PROPOSAL = "entity_proposal"
    EVALUATION = "evaluation"
    EVIDENCE = "evidence"
    LINEAGE = "lineage"
    RELATIONSHIP_PROPOSAL = "relationship_proposal"
    SUPERSESSION_PROPOSAL = "supersession_proposal"

    def __str__(self) -> str:
        return str(self.value)
