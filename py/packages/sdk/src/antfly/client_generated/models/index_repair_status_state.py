from enum import StrEnum


class IndexRepairStatusState(StrEnum):
    FAILED = "failed"
    PAUSED = "paused"
    REBUILDING = "rebuilding"
    WAITING = "waiting"

    def __str__(self) -> str:
        return str(self.value)
