from enum import Enum


class GraphWorkBudgetExceededErrorDimension(str, Enum):
    EXPLORED_EDGES = "explored_edges"
    EXPLORED_EDGE_BYTES = "explored_edge_bytes"
    EXPLORED_NODES = "explored_nodes"
    INTERMEDIATE_STATES = "intermediate_states"

    def __str__(self) -> str:
        return str(self.value)
