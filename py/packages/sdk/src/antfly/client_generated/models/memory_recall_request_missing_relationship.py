from enum import StrEnum


class MemoryRecallRequestMissingRelationship(StrEnum):
    DEPENDS_ON = "depends_on"
    MOTIVATES = "motivates"
    REJECTED_IN_FAVOR_OF = "rejected_in_favor_of"
    SUPERSEDES = "supersedes"

    def __str__(self) -> str:
        return str(self.value)
