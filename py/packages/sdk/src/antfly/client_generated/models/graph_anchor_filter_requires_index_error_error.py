from enum import Enum


class GraphAnchorFilterRequiresIndexErrorError(str, Enum):
    GRAPH_ANCHOR_FILTER_REQUIRES_INDEX = "graph_anchor_filter_requires_index"

    def __str__(self) -> str:
        return str(self.value)
