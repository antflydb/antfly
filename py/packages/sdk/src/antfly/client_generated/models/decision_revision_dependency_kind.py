from enum import StrEnum


class DecisionRevisionDependencyKind(StrEnum):
    CASCADE = "cascade"
    MODEL = "model"
    UNVERIFIED = "unverified"

    def __str__(self) -> str:
        return str(self.value)
