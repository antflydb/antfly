from enum import StrEnum


class GraphMetricBuildPageStatusState(StrEnum):
    COMPLETE = "complete"
    FAILED = "failed"
    LEASED = "leased"
    PENDING = "pending"

    def __str__(self) -> str:
        return str(self.value)
