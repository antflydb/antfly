from enum import StrEnum


class GraphMatchOperationLimitExceededErrorError(StrEnum):
    GRAPH_MATCH_OPERATION_LIMIT_EXCEEDED = "graph_match_operation_limit_exceeded"

    def __str__(self) -> str:
        return str(self.value)
