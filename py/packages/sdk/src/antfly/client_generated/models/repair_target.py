from enum import Enum


class RepairTarget(str, Enum):
    ARTIFACT = "artifact"
    INDEX = "index"

    def __str__(self) -> str:
        return str(self.value)
