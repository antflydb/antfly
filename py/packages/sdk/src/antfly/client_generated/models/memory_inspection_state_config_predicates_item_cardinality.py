from enum import StrEnum


class MemoryInspectionStateConfigPredicatesItemCardinality(StrEnum):
    MANY = "many"
    ONE = "one"

    def __str__(self) -> str:
        return str(self.value)
