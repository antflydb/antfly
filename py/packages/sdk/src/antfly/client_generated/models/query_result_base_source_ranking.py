from enum import StrEnum


class QueryResultBaseSourceRanking(StrEnum):
    ORDERED = "ordered"
    RRF = "rrf"

    def __str__(self) -> str:
        return str(self.value)
