from enum import Enum


class TransactionConflictParticipantPhase(str, Enum):
    BEGIN = "begin"
    PREPARE = "prepare"
    RESOLVE = "resolve"

    def __str__(self) -> str:
        return str(self.value)
