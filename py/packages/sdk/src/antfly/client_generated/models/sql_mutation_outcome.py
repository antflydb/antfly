from enum import StrEnum


class SQLMutationOutcome(StrEnum):
    COMMITTED = "committed"
    COMMITTED_GRAPH_METRIC_MATERIALIZATION_REJECTED = "committed_graph_metric_materialization_rejected"
    COMMITTED_PENDING = "committed_pending"
    COMMITTED_REPAIR_REQUIRED = "committed_repair_required"

    def __str__(self) -> str:
        return str(self.value)
