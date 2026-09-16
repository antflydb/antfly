from enum import StrEnum


class ExtractionJointConstraintMaxRelationsPerTailType(StrEnum):
    MAXRELATIONSPERTAIL = "MaxRelationsPerTail"

    def __str__(self) -> str:
        return str(self.value)
