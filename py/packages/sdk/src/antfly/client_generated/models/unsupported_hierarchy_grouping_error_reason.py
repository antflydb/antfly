from enum import Enum


class UnsupportedHierarchyGroupingErrorReason(str, Enum):
    MIXED_DOCUMENT_CHUNK_SOURCES = "mixed_document_chunk_sources"

    def __str__(self) -> str:
        return str(self.value)
