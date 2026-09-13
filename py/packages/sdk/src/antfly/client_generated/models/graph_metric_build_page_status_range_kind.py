from enum import StrEnum


class GraphMetricBuildPageStatusRangeKind(StrEnum):
    CONTRIBUTIONS = "contributions"
    FULL = "full"
    JOB_CONTROL = "job_control"
    NODES = "nodes"
    REVERSE_EDGES = "reverse_edges"
    SCORES = "scores"
    SUMMARY = "summary"

    def __str__(self) -> str:
        return str(self.value)
