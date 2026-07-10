from enum import Enum


class QueryHitsTotalRelation(str, Enum):
    EXACT = "exact"
    GTE = "gte"

    def __str__(self) -> str:
        return str(self.value)
