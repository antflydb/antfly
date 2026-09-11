from enum import StrEnum


class RepairTarget(StrEnum):
    ARTIFACT = "artifact"
    INDEX = "index"

    def __str__(self) -> str:
        return str(self.value)
