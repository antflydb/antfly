from enum import StrEnum


class GraphMetricConfigKind(StrEnum):
    DEGREE = "degree"
    EIGENVECTOR = "eigenvector"
    HITS_AUTHORITY = "hits_authority"
    HITS_HUB = "hits_hub"
    PAGERANK = "pagerank"

    def __str__(self) -> str:
        return str(self.value)
