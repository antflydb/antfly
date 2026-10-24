from enum import StrEnum


class MemoryEntityCandidatePageKind(StrEnum):
    IDENTITY = "identity"
    RELATIONSHIP = "relationship"

    def __str__(self) -> str:
        return str(self.value)
