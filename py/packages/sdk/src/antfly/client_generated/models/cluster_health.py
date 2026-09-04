from enum import StrEnum


class ClusterHealth(StrEnum):
    DEGRADED = "degraded"
    ERROR = "error"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
