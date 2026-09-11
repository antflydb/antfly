from enum import StrEnum


class ExtractionJointConstraintInverseRelationType(StrEnum):
    INVERSERELATION = "InverseRelation"

    def __str__(self) -> str:
        return str(self.value)
