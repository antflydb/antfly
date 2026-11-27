from enum import StrEnum


class SeedOutcomeOutcome(StrEnum):
    ACCEPTED = "accepted"
    CONFLICT = "conflict"
    DELETED = "deleted"
    MISSING = "missing"
    PENDING = "pending"
    REJECTED = "rejected"
    REPLACED = "replaced"
    UNCHANGED = "unchanged"

    def __str__(self) -> str:
        return str(self.value)
