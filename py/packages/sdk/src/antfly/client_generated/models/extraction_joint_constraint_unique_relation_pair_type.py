from enum import StrEnum


class ExtractionJointConstraintUniqueRelationPairType(StrEnum):
    UNIQUERELATIONPAIR = "UniqueRelationPair"

    def __str__(self) -> str:
        return str(self.value)
