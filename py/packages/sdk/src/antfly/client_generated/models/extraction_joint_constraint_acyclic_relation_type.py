from enum import StrEnum


class ExtractionJointConstraintAcyclicRelationType(StrEnum):
    ACYCLICRELATION = "AcyclicRelation"

    def __str__(self) -> str:
        return str(self.value)
