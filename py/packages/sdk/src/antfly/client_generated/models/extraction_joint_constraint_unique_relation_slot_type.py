from enum import StrEnum


class ExtractionJointConstraintUniqueRelationSlotType(StrEnum):
    UNIQUERELATIONSLOT = "UniqueRelationSlot"

    def __str__(self) -> str:
        return str(self.value)
