from enum import Enum


class GraphDistinctBudgetExceededErrorError(str, Enum):
    GRAPH_DISTINCT_BUDGET_EXCEEDED = "graph_distinct_budget_exceeded"

    def __str__(self) -> str:
        return str(self.value)
