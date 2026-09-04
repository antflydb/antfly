from enum import StrEnum


class TransactionConflictParticipantPhase(StrEnum):
    BEGIN = "begin"
    PREPARE = "prepare"
    RESOLVE = "resolve"

    def __str__(self) -> str:
        return str(self.value)
