from enum import Enum


class GraphSourceArtifactStatusFormat(str, Enum):
    EXTRACTION_GRAPH = "extraction_graph"
    EXTRACTION_RELATION = "extraction_relation"

    def __str__(self) -> str:
        return str(self.value)
