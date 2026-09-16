from enum import StrEnum


class ExtractionLongDocumentMetadataSolverOptimalityScope(StrEnum):
    RETAINED_CANDIDATE_GRAPH = "retained_candidate_graph"

    def __str__(self) -> str:
        return str(self.value)
