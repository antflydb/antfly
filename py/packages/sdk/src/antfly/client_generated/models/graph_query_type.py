from enum import Enum


class GraphQueryType(str, Enum):
    K_SHORTEST_PATHS = "k_shortest_paths"
    NEIGHBORS = "neighbors"
    PATTERN = "pattern"
    SHORTEST_PATH = "shortest_path"
    TRAVERSE = "traverse"

    def __str__(self) -> str:
        return str(self.value)
