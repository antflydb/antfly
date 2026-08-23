from enum import Enum


class GraphCrossRangeModeUnsupportedErrorError(str, Enum):
    GRAPH_CROSS_RANGE_MODE_UNSUPPORTED = "graph_cross_range_mode_unsupported"

    def __str__(self) -> str:
        return str(self.value)
