from enum import StrEnum


class GraphQueryUnsupportedErrorError(StrEnum):
    GRAPH_QUERY_UNSUPPORTED = "graph_query_unsupported"

    def __str__(self) -> str:
        return str(self.value)
