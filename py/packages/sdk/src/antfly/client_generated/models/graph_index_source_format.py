from enum import Enum


class GraphIndexSourceFormat(str, Enum):
    EXTRACTION_GRAPH = "extraction_graph"
    EXTRACTION_RELATION = "extraction_relation"

    def __str__(self) -> str:
        return str(self.value)
