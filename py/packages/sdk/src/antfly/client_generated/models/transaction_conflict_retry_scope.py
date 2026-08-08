from enum import Enum


class TransactionConflictRetryScope(str, Enum):
    DOC_IDENTITY = "doc_identity"
    PARTICIPANT = "participant"
    SESSION = "session"
    TOPOLOGY = "topology"

    def __str__(self) -> str:
        return str(self.value)
