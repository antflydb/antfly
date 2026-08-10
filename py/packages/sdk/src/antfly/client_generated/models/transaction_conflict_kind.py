from enum import Enum


class TransactionConflictKind(str, Enum):
    DOC_IDENTITY_UNAVAILABLE = "doc_identity_unavailable"
    INTENT_CONFLICT = "intent_conflict"
    PARTICIPANT_UNAVAILABLE = "participant_unavailable"
    SESSION_LEASE_LOST = "session_lease_lost"
    TOPOLOGY_CHANGED = "topology_changed"
    TORN_STATE = "torn_state"
    TRANSACTION_CONFLICT = "transaction_conflict"
    VERSION_CONFLICT = "version_conflict"

    def __str__(self) -> str:
        return str(self.value)
