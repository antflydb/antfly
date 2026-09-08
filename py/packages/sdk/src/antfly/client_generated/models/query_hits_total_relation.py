from enum import StrEnum


class QueryHitsTotalRelation(StrEnum):
    EXACT = "exact"
    GTE = "gte"

    def __str__(self) -> str:
        return str(self.value)
