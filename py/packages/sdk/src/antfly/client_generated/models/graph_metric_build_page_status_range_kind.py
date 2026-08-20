from enum import Enum


class GraphMetricBuildPageStatusRangeKind(str, Enum):
    CONTRIBUTIONS = "contributions"
    FULL = "full"
    JOB_CONTROL = "job_control"
    NODES = "nodes"
    REVERSE_EDGES = "reverse_edges"
    SCORES = "scores"

    def __str__(self) -> str:
        return str(self.value)
