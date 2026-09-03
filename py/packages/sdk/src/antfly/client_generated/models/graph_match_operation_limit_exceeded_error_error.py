from enum import Enum


class GraphMatchOperationLimitExceededErrorError(str, Enum):
    GRAPH_MATCH_OPERATION_LIMIT_EXCEEDED = "graph_match_operation_limit_exceeded"

    def __str__(self) -> str:
        return str(self.value)
