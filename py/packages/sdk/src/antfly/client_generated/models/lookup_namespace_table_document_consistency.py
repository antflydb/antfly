from enum import StrEnum


class LookupNamespaceTableDocumentConsistency(StrEnum):
    READ_INDEX = "read_index"
    STALE = "stale"

    def __str__(self) -> str:
        return str(self.value)
