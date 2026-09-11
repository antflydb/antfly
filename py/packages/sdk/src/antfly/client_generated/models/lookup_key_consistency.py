from enum import StrEnum


class LookupKeyConsistency(StrEnum):
    LEADER_LEASE = "leader_lease"
    READ_INDEX = "read_index"
    STALE = "stale"

    def __str__(self) -> str:
        return str(self.value)
