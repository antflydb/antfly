from enum import StrEnum


class MemoryInspectionStatePendingConfigType0PredicatesItemCardinality(StrEnum):
    MANY = "many"
    ONE = "one"

    def __str__(self) -> str:
        return str(self.value)
