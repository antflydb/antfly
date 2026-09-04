from enum import StrEnum


class GraphArtifactSourceConfigFormat(StrEnum):
    EXTRACTION_GRAPH = "extraction_graph"
    EXTRACTION_RELATION = "extraction_relation"

    def __str__(self) -> str:
        return str(self.value)
