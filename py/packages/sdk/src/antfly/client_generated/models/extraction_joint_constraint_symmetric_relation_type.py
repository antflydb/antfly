from enum import StrEnum


class ExtractionJointConstraintSymmetricRelationType(StrEnum):
    SYMMETRICRELATION = "SymmetricRelation"

    def __str__(self) -> str:
        return str(self.value)
