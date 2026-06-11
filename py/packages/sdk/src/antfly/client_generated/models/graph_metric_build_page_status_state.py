from enum import Enum


class GraphMetricBuildPageStatusState(str, Enum):
    COMPLETE = "complete"
    FAILED = "failed"
    LEASED = "leased"
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
