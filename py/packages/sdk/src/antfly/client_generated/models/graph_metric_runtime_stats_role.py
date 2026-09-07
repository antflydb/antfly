from enum import StrEnum


class GraphMetricRuntimeStatsRole(StrEnum):
    COMBINED = "combined"
    COORDINATOR = "coordinator"
    WORKER = "worker"
    WORKER_POOL = "worker_pool"

    def __str__(self) -> str:
        return str(self.value)
