from enum import StrEnum


class RelationalIndexBuildState(StrEnum):
    BUILDING = "building"
    FAILED = "failed"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
