from enum import StrEnum


class TransactionConflictRetryScope(StrEnum):
    DOC_IDENTITY = "doc_identity"
    PARTICIPANT = "participant"
    SESSION = "session"
    TOPOLOGY = "topology"

    def __str__(self) -> str:
        return str(self.value)
