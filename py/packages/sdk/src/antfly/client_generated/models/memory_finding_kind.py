from enum import StrEnum


class MemoryFindingKind(StrEnum):
    DANGLING_LINK = "dangling_link"
    DISPUTE = "dispute"
    DUPLICATE = "duplicate"
    HARMFUL = "harmful"
    ORPHAN_ENTITY = "orphan_entity"
    PROMOTE = "promote"
    STALE = "stale"
    UNORDERED = "unordered"
    UNVERIFIED = "unverified"

    def __str__(self) -> str:
        return str(self.value)
