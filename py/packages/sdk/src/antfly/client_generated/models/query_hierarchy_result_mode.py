from enum import Enum


class QueryHierarchyResultMode(str, Enum):
    MATCHES = "matches"
    SOURCES = "sources"

    def __str__(self) -> str:
        return str(self.value)
