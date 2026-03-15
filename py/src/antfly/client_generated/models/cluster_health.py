from enum import Enum


class ClusterHealth(str, Enum):
    DEGRADED = "degraded"
    ERROR = "error"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
