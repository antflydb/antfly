from enum import StrEnum


class GlobalStatefulQueryRequestSourceRanking(StrEnum):
    RRF = "rrf"

    def __str__(self) -> str:
        return str(self.value)
