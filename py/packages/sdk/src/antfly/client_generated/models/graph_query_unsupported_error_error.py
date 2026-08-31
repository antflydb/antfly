from enum import Enum


class GraphQueryUnsupportedErrorError(str, Enum):
    GRAPH_QUERY_UNSUPPORTED = "graph_query_unsupported"

    def __str__(self) -> str:
        return str(self.value)
