from enum import StrEnum


class ExtractionJointConstraintNoSelfLoopsType(StrEnum):
    NOSELFLOOPS = "NoSelfLoops"

    def __str__(self) -> str:
        return str(self.value)
