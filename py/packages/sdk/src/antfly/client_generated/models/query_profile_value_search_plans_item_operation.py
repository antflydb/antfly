from enum import StrEnum


class QueryProfileValueSearchPlansItemOperation(StrEnum):
    CONTAINS = "contains"
    EQUALS = "equals"
    MATCHES = "matches"

    def __str__(self) -> str:
        return str(self.value)
