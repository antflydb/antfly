from enum import StrEnum


class DecisionCascadeStageTraceDecisionMethod(StrEnum):
    GENERATED = "generated"
    TYPED = "typed"

    def __str__(self) -> str:
        return str(self.value)
