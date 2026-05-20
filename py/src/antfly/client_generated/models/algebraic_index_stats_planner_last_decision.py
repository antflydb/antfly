from enum import Enum


class AlgebraicIndexStatsPlannerLastDecision(str, Enum):
    FALLBACK = "fallback"
    SELECTED = "selected"

    def __str__(self) -> str:
        return str(self.value)
