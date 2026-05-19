from enum import Enum


class AlgebraicIndexStatsIndexType(str, Enum):
    ALGEBRAIC = "algebraic"

    def __str__(self) -> str:
        return str(self.value)
