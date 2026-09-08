from enum import StrEnum


class GraphWorkBudgetExceededErrorError(StrEnum):
    GRAPH_WORK_BUDGET_EXCEEDED = "graph_work_budget_exceeded"

    def __str__(self) -> str:
        return str(self.value)
