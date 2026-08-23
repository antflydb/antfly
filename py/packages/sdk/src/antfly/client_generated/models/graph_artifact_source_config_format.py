from enum import Enum


class GraphArtifactSourceConfigFormat(str, Enum):
    EXTRACTION_GRAPH = "extraction_graph"
    EXTRACTION_RELATION = "extraction_relation"

    def __str__(self) -> str:
        return str(self.value)
