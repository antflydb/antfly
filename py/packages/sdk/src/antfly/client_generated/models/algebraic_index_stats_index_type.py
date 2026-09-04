from enum import StrEnum


class AlgebraicIndexStatsIndexType(StrEnum):
    ALGEBRAIC = "algebraic"

    def __str__(self) -> str:
        return str(self.value)
