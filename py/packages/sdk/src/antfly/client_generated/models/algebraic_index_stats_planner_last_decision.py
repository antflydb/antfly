from enum import StrEnum


class AlgebraicIndexStatsPlannerLastDecision(StrEnum):
    FALLBACK = "fallback"
    SELECTED = "selected"

    def __str__(self) -> str:
        return str(self.value)
