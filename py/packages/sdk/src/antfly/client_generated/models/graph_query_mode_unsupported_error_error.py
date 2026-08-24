from enum import Enum


class GraphQueryModeUnsupportedErrorError(str, Enum):
    GRAPH_QUERY_MODE_UNSUPPORTED = "graph_query_mode_unsupported"

    def __str__(self) -> str:
        return str(self.value)
