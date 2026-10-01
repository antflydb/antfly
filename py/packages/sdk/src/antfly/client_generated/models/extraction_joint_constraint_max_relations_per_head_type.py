from enum import StrEnum


class ExtractionJointConstraintMaxRelationsPerHeadType(StrEnum):
    MAXRELATIONSPERHEAD = "MaxRelationsPerHead"

    def __str__(self) -> str:
        return str(self.value)
