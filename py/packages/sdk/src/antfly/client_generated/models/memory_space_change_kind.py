from enum import StrEnum


class MemorySpaceChangeKind(StrEnum):
    ASSERTION_CHANGED = "assertion_changed"
    ASSERTION_DISPUTED = "assertion_disputed"
    ASSERTION_RETRACTED = "assertion_retracted"
    ASSERTION_SUPERSEDED = "assertion_superseded"
    CONFIGURATION_CHANGED = "configuration_changed"
    CONTEXT_INVALIDATED = "context_invalidated"
    ENTITY_CHANGED = "entity_changed"
    EVALUATION_CHANGED = "evaluation_changed"
    EVIDENCE_OBSERVED = "evidence_observed"
    LINEAGE_CHANGED = "lineage_changed"
    PROPOSAL_DECIDED = "proposal_decided"
    PROPOSAL_DEFERRED = "proposal_deferred"
    PROPOSAL_OPENED = "proposal_opened"

    def __str__(self) -> str:
        return str(self.value)
