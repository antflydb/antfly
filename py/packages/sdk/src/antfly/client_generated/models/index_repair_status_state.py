from enum import Enum


class IndexRepairStatusState(str, Enum):
    FAILED = "failed"
    PAUSED = "paused"
    REBUILDING = "rebuilding"
    WAITING = "waiting"

    def __str__(self) -> str:
        return str(self.value)
