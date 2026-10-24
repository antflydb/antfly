from enum import StrEnum


class MemorySpaceConfigPredicatesItemCardinality(StrEnum):
    MANY = "many"
    ONE = "one"

    def __str__(self) -> str:
        return str(self.value)
