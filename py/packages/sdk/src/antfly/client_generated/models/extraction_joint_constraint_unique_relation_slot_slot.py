from enum import StrEnum


class ExtractionJointConstraintUniqueRelationSlotSlot(StrEnum):
    HEAD = "head"
    SLOT = "slot"
    TAIL = "tail"

    def __str__(self) -> str:
        return str(self.value)
