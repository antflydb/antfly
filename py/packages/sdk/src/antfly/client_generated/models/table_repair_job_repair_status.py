from enum import Enum


class TableRepairJobRepairStatus(str, Enum):
    COMPLETE = "complete"
    DEBT_REMAINING = "debt_remaining"
    IN_PROGRESS = "in_progress"
    STOPPED = "stopped"

    def __str__(self) -> str:
        return str(self.value)
